#!/bin/bash
#
# Copied from the standard CCP install_gpdb.sh script.
#
set -euo pipefail

# Set the DEBUG_UPGRADE envvar to a nonempty value to get (extremely) verbose
# output.
DEBUG_UPGRADE=${DEBUG_UPGRADE:-}

./ccp_src/scripts/setup_ssh_to_cluster.sh

# The host running the master GPDB segment.
MASTER_HOST=mdw

# The GPHOME directories (containing greenplum_path.sh, the bin/ directory, and
# so on) for the old and new clusters, respectively.
OLD_GPHOME=/usr/local/greenplum-db-devel
NEW_GPHOME=/usr/local/gpdb_master

# The old and new clusters' master data directories.
OLD_MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
NEW_MASTER_DATA_DIRECTORY=/data/gpdata/master-new/gpseg-1

DIRNAME=$(dirname "$0")

cat << EOF
  ############################
  #                          #
  #  New GPDB Installation   #
  #                          #
  ############################
EOF

load_old_db_data() {
    # Copy the SQL dump over to the master host and load it into the database.
    local dumpfile=$1
    local psql_env="PGOPTIONS='--client-min-messages=warning'"
    local psql_opts="-q"

    if [ -n "$DEBUG_UPGRADE" ]; then
        # Don't quiet psql when debugging.
        psql_env=
        psql_opts=
    fi

    echo 'Loading test database...'

    scp "$dumpfile" ${MASTER_HOST}:/tmp/dump.sql.xz
    ssh -n ${MASTER_HOST} '
        source '"${OLD_GPHOME}"'/greenplum_path.sh
        unxz < /tmp/dump.sql.xz | '"${psql_env}"' psql '"${psql_opts}"' -f - postgres
    '
}

dump_cluster() {
    # Dump the entire cluster contents to file, using the new pg_dumpall.
    local dumpfile=$1

    ssh -n ${MASTER_HOST} "
        source ${NEW_GPHOME}/greenplum_path.sh
        pg_dumpall -f '$dumpfile'
    "
}

extract_gpdb_tarball() {
    local node_hostname=$1
    local tarball_dir=$2
    # Commonly the incoming binary will be called bin_gpdb.tar.gz. Because many other teams/pipelines tend to use 
    # that naming convention we are not, deliberately. Once the file crosses into our domain, we will not use
    # the conventional name.  This should make clear that we will install any valid binary, not just those that follow
    # the naming convention.
    scp ${tarball_dir}/*.tar.gz $node_hostname:/tmp/gpdb_binary.tar.gz
    ssh -ttn $node_hostname "sudo bash -c \"\
      mkdir -p ${NEW_GPHOME}; \
      tar -xf /tmp/gpdb_binary.tar.gz -C ${NEW_GPHOME}; \
      chown -R gpadmin:gpadmin ${NEW_GPHOME}; \
      sed -ie 's|^GPHOME=.*|GPHOME=${NEW_GPHOME}|' ${NEW_GPHOME}/greenplum_path.sh ; \
    \""
}

create_new_datadir() {
    local node_hostname=$1

    # Create a -new directory for every data directory that already exists.
    # This is what we'll be init'ing the new database into.
    ssh -ttn "$node_hostname" 'sudo bash -c '\''
        for dir in $(find /data/gpdata/* -maxdepth 0 -type d); do
            newdir="${dir}-new"

            mkdir -p "$newdir"
            chown gpadmin:gpadmin "$newdir"
        done
    '\'''
}

gpinitsystem_for_upgrade() {
    # Stop the old cluster and init a new one.
    #
    # FIXME: the checksum/string settings below need to be pulled from the
    # previous database, not hardcoded. And long-term, we need to decide how
    # Greenplum clusters should be upgraded when GUC settings' defaults change.
    ssh -ttn ${MASTER_HOST} '
        source '"${OLD_GPHOME}"'/greenplum_path.sh
        gpstop -a -d '"${OLD_MASTER_DATA_DIRECTORY}"'

        source '"${NEW_GPHOME}"'/greenplum_path.sh
        sed -e '\''s|\(/data/gpdata/\w\+\)|\1-new|g'\'' gpinitsystem_config > gpinitsystem_config_new
        # echo "HEAP_CHECKSUM=off" >> gpinitsystem_config_new
        # echo "standard_conforming_strings = off" >> upgrade_addopts
        # echo "escape_string_warning = off" >> upgrade_addopts
        gpinitsystem -a -c ~gpadmin/gpinitsystem_config_new -h ~gpadmin/segment_host_list # -p ~gpadmin/upgrade_addopts
        gpstop -a -d '"${NEW_MASTER_DATA_DIRECTORY}"'
    '
}

# run_upgrade hostname data-directory [options]
run_upgrade() {
    # Runs pg_upgrade on a host for the given data directory. The new data
    # directory is assumed to follow the *-new convention established by
    # gpinitsystem_for_upgrade(), above.

    local hostname=$1
    local datadir=$2
    shift 2

    local upgrade_opts=

    if [ -n "$DEBUG_UPGRADE" ]; then
        upgrade_opts="--verbose"
    fi

    ssh -ttn "$hostname" '
        source '"${NEW_GPHOME}"'/greenplum_path.sh
        time pg_upgrade '"${upgrade_opts}"' '"$*"' \
            -b '"${OLD_GPHOME}"'/bin/ -B '"${NEW_GPHOME}"'/bin/ \
            -d '"$datadir"' \
            -D '"$(sed -e 's|\(/data/gpdata/\w\+\)|\1-new|g' <<< "$datadir")"
}

dump_old_master_query() {
    # Prints the rows generated by the given SQL query to stdout. The query is
    # run on the old master, pre-upgrade.
    ssh -n ${MASTER_HOST} '
        source '"${OLD_GPHOME}"'/greenplum_path.sh
        psql postgres --quiet --no-align --tuples-only -F"'$'\t''" -c "'$1'"
    '
}

get_segment_datadirs() {
    # Prints the hostnames and data directories of each primary and mirror: one
    # instance per line, with the hostname and data directory separated by a
    # tab.

    # First try dumping the 6.0 version...
    local q="SELECT hostname, datadir FROM gp_segment_configuration WHERE content <> -1"
    if ! dump_old_master_query "$q" 2>/dev/null; then
        # ...and then fall back to pre-6.0.
        q="SELECT hostname, fselocation FROM gp_segment_configuration JOIN pg_catalog.pg_filespace_entry ON (dbid = fsedbid) WHERE content <> -1"
        dump_old_master_query "$q"
    fi
}

start_upgraded_cluster() {
    ssh -n ${MASTER_HOST} "
        source ${NEW_GPHOME}/greenplum_path.sh
        MASTER_DATA_DIRECTORY='${NEW_MASTER_DATA_DIRECTORY}' gpstart -a -v
    "
}

apply_sql_fixups() {
    local psql_env=
    local psql_opts="-v ON_ERROR_STOP=1"

    if [ -n "$DEBUG_UPGRADE" ]; then
        # Don't quiet psql when debugging.
        psql_opts+=" -e"
    else
        psql_env="PGOPTIONS='--client-min-messages=warning'"
        psql_opts+=" -q"
    fi

    echo 'Finalizing upgrade...'

    # FIXME: we need a generic way for gp_upgrade to figure out which SQL fixup
    # files need to be applied to the cluster before it is used.
    ssh -n ${MASTER_HOST} '
        source '"${NEW_GPHOME}"'/greenplum_path.sh
        if [ -f reindex_all.sql ]; then
            '"${psql_env}"' psql '"${psql_opts}"' -f reindex_all.sql template1
        fi
    '
}

# Compares the old and new pg_dumpall output (after running them both through
# dumpsort).
compare_dumps() {
    local old_dump=$1
    local new_dump=$2

    scp "$DIRNAME/dumpsort.gawk" ${MASTER_HOST}:~

    ssh -n ${MASTER_HOST} "
        diff -U3 --speed-large-files --ignore-space-change \
            <(gawk -f ~/dumpsort.gawk < '$old_dump') \
            <(gawk -f ~/dumpsort.gawk < '$new_dump')
    "
}

CLUSTER_NAME=$(cat ./terraform*/name)

GPDB_TARBALL_DIR=${1:-}

if [ -z "${GPDB_TARBALL_DIR}" ]; then
  echo "Using default directory"
fi

SQLDUMP_FILE=${2:-}

if [ -z "${SQLDUMP_FILE}" ]; then
  echo "Using default SQL dump"
fi

# Use the third argument for the number of hosts to connect to; if that's not
# given, fall back to the NUMBER_OF_NODES environment variable.
NUMBER_OF_NODES=${3:-${NUMBER_OF_NODES:-}}

if [ -z ${NUMBER_OF_NODES} ]; then
  echo "Number of nodes must be supplied to this script"
  exit 1
fi

old_dump=/tmp/pre_upgrade.sql
new_dump=/tmp/post_upgrade.sql

set -v

time load_old_db_data ${SQLDUMP_FILE:-sqldump/dump.sql.xz}

for ((i=0; i<${NUMBER_OF_NODES}; ++i)); do
  extract_gpdb_tarball ccp-${CLUSTER_NAME}-$i ${GPDB_TARBALL_DIR:-gpdb_binary}
  create_new_datadir ccp-${CLUSTER_NAME}-$i
done

time dump_cluster "$old_dump"
get_segment_datadirs > /tmp/segment_datadirs.txt
gpinitsystem_for_upgrade

# TODO: we need to switch the mode argument according to GPDB version
echo "Upgrading master at ${MASTER_HOST}..."
run_upgrade ${MASTER_HOST} "${OLD_MASTER_DATA_DIRECTORY}" --mode=dispatcher

while read -u30 hostname datadir; do
    echo "Upgrading segment at '$hostname' ($datadir)..."

    newdatadir=$(sed -e 's|\(/data/gpdata/\w\+\)|\1-new|g' <<< "$datadir")

    # NOTE: the trailing slash on the rsync source directory is important! It
    # means to transfer the directory's contents and not the directory itself.
    ssh -n ${MASTER_HOST} rsync -r --delete "${NEW_MASTER_DATA_DIRECTORY}/" "$hostname:$newdatadir" \
        --exclude /postgresql.conf \
        --exclude /pg_hba.conf \
        --exclude /postmaster.opts \
        --exclude /gp_replication.conf \
        --exclude /gp_dbid \
        --exclude /gpssh.conf \
        --exclude /gpperfmon/

    run_upgrade "$hostname" "$datadir" --mode=segment
done 30< /tmp/segment_datadirs.txt

start_upgraded_cluster
time apply_sql_fixups
time dump_cluster "$new_dump"

if ! compare_dumps "$old_dump" "$new_dump"; then
    echo 'error: before and after dumps differ'
    exit 1
fi

echo Complete

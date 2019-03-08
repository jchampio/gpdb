use strict;
use warnings;

use PostgresNode;
use TestLib;

use Test::More tests => 1;

sub directory_contents
{
	my $path = shift;

	opendir my $dir, $path or die "couldn't open directory $path: $!";
	my @files = readdir $dir;
	closedir $dir;

	return @files;
}

note "setting up data directory...";
my $node = get_new_node('master');
$node->init;

# Re-enable the GPDB logging collector (which is disabled by default for the TAP
# tests) so that we can test the shipped logging behavior.
$node->{_enable_logging_collector} = 1;

# Set up logs to rotate once per minute. Unfortunately, the regression we're
# looking for can only be triggered during a natural rotation, not manually
# using pg_rotate_logfile() or similar.
$node->append_conf('postgresql.conf', "log_rotation_age = 1\n");

$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

my $datadir = $node->data_dir;
my $logpath = "$datadir/pg_log";

my @logs = directory_contents($logpath);

print "@logs\n";

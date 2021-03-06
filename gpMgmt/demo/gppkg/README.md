# Pre-Requirements

```
yum -y install rpmdevtools rpmlint
```


# How to Create a Greenplum Package that can be manipulated with *gppkg* 

Run the following script on Centos6 or Redhat6. The destination directory is hardcoded for simplicity.

```
DIR=/tmp/sample

mkdir -p ${DIR}
cp gppkg_spec.yml ${DIR}
cp sample-sources.tar.gz ${DIR}
cp sample.spec ${DIR}

./generate_sample_gppkg.sh ${DIR} 
```

Your sample package will be at:

`ls /tmp/sample-gppkg/sample.gppkg`

# Using Makefile to create a Greenplum Package

To integrate build gppkg into a Makefile, it needs to include the following .mk file
```
# Ubuntu
include gppkg_deb.mk
# CcentOS/Redhat
include gppkg_rpm.mk
```

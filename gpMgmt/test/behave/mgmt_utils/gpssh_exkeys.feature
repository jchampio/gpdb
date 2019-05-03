@gpssh-exkeys
Feature: gpssh behave tests

    @concourse_cluster
    Scenario: N-to-N exchange works
        Given the gpssh-exkeys master host is set to "mdw"
          And the gpssh-exkeys segment host is set to "sdw1,sdw2,sdw3"
          And all SSH configurations are backed up and removed
          And the segments can only be accessed using the master key
          And there is no duplication in the "authorized_keys" files
         Then all hosts "cannot" reach each other or themselves automatically

         When gpssh-exkeys is run successfully
         Then all hosts "can" reach each other or themselves automatically

         When gpssh-exkeys is run successfully
         Then all hosts "can" reach each other or themselves automatically
          And there is no duplication in the "known_hosts" files
          And there is no duplication in the "authorized_keys" files

    @concourse_cluster
    Scenario: additional hosts may be added after initial run
        Given the gpssh-exkeys master host is set to "mdw"
          And the gpssh-exkeys segment host is set to "sdw1,sdw2,sdw3"
          And all SSH configurations are backed up and removed
          And the segments can only be accessed using the master key
          And there is no duplication in the "authorized_keys" files
         Then all hosts "cannot" reach each other or themselves automatically

         When gpssh-exkeys is run successfully on hosts "sdw1, sdw2"
          And gpssh-exkeys is run successfully on additional hosts "sdw3"
         Then all hosts "can" reach each other or themselves automatically
          And there is no duplication in the "known_hosts" files
          And there is no duplication in the "authorized_keys" files

    @concourse_cluster
    Scenario: hostfiles are accepted as well
        Given the gpssh-exkeys master host is set to "mdw"
          And the gpssh-exkeys segment host is set to "sdw1,sdw2,sdw3"
          And all SSH configurations are backed up and removed
          And the segments can only be accessed using the master key
          And there is no duplication in the "authorized_keys" files
         Then all hosts "cannot" reach each other or themselves automatically

         When gpssh-exkeys is run successfully with a hostfile
         Then all hosts "can" reach each other or themselves automatically

    @skip
    @concourse_cluster
    Scenario: IPv6 addresses are accepted
        Given the gpssh-exkeys master host is set to "mdw"
          And the gpssh-exkeys segment host is set to "sdw1,sdw2,sdw3"
          And all SSH configurations are backed up and removed
          And the segments can only be accessed using the master key
          And there is no duplication in the "authorized_keys" files
         Then all hosts "cannot" reach each other or themselves automatically

         When gpssh-exkeys is run successfully with IPv6 addresses
         Then all hosts "can" reach each other or themselves automatically

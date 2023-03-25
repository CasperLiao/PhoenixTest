PhoneixTest-1.0-SNAPSHOT-jar-with-dependencies.jarorg.example.App## Getting Started

This is an example of how you may give instructions on setting up your project locally.
- Clone the repo

```commandlinejava -cp .:./target/test-0.0.1-SNAPSHOT-jar-with-dependencies.jar --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED test.Demo  user1@AMDEV.COM user1.keytab users_data
 $ git clone https://github.com/jjhuangw/iris_phoenix_hbase
```
- Set up Kerberos environment in your local    
    - copy krb5.conf to /etc/   
    - add hbase.zookeeper.quorum host mapping to /etc/hosts   
- Pack up the demo project   

```commandline
$ mvn clean compile assembly:single
$ java -cp .:./target/PhoneixTest-1.0-SNAPSHOT-jar-with-dependencies.jar org.example.App  user1@AMDEV.COM user1.keytab users_data

# if not work, please add --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED in your command.
$ java -cp .:./target/PhoneixTest-1.0-SNAPSHOT-jar-with-dependencies.jar --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED org.example.App  user1@AMDEV.COM user1.keytab users_data
```

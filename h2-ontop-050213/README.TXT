=====================================================================
                         Information
=====================================================================

H2 Server version: H2 1.3.168 (2012-07-13)

This H2 server is distributed with the -ontop- framework for 
ontology based data access. 

The server contains the following predefined databases:
 - bookstores.h2.db
 - enrollment.h2.db
 - books.h2.db
 - helloworld.h2.db

The databases are located in the ./ folder and have extension h2.db
The databases have been created enabling case sensitivity in H2. To
use the databases you should use the parameter
DATABASE_TO_UPPER=FALSE in the JDBC URL.

For example:

jdbc:h2:tcp://localhost/bookstores;DATABASE_TO_UPPER=FALSE

jdbc:h2:tcp://localhost/enrollment;DATABASE_TO_UPPER=FALSE

jdbc:h2:tcp://localhost/books;DATABASE_TO_UPPER=FALSE

jdbc:h2:tcp://localhost/helloworld;DATABASE_TO_UPPER=FALSE


These databases are used in the tutorials located at

https://babbage.inf.unibz.it/trac/obdapublic/wiki

You can also browse the databases content using H2 browser.

-
Mariano







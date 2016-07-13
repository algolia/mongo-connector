**WARNING** : This connector is deprecated, please use an [API Client](https://www.algolia.com/doc/).

For complete documentation, check out the `Mongo Connector Wiki <https://github.com/10gen-labs/mongo-connector/wiki>`__.

DISCLAIMER
----------

Please note: all tools/ scripts in this repo are released for use "AS IS" without any warranties of any kind, including, but not limited to their installation, use, or performance. We disclaim any and all warranties, either express or implied, including but not limited to any warranty of noninfringement, merchantability, and/ or fitness for a particular purpose. We do not warrant that the technology will meet your requirements, that the operation thereof will be uninterrupted or error-free, or that any errors will be corrected.
Any use of these scripts and tools is at your own risk. There is no guarantee that they have been through thorough testing in a comparable environment and we are not responsible for any damage or data loss incurred with their use.
You are responsible for reviewing and testing any scripts you run thoroughly before use in any non-testing environment.

System Overview
---------------

mongo-connector creates a pipeline from a MongoDB cluster to one or more
target systems, such as Solr, Elasticsearch, or another MongoDB cluster.
By tailing the MongoDB oplog, it replicates operations from MongoDB to
these systems in real-time. It has been tested with Python 2.6, 2.7,
3.3, and 3.4. Detailed documentation is available on the
`wiki <https://github.com/10gen-labs/mongo-connector/wiki>`__.

Getting Started
---------------

Installation
~~~~~~~~~~~~

You can install the development version of mongo-connector
manually::

  git clone https://github.com/algolia/mongo-connector.git
  cd mongo-connector
  python setup.py install

You may have to run ``python setup.py install`` with ``sudo``, depending
on where you're installing mongo-connector and what privileges you have.

Using mongo-connector
~~~~~~~~~~~~~~~~~~~~~

mongo-connector replicates operations from the MongoDB oplog, so a
`replica
set <http://docs.mongodb.org/manual/tutorial/deploy-replica-set/>`__
must be running before startup. For development purposes, you may find
it convenient to run a one-node replica set (note that this is **not**
recommended for production)::

  mongod --replSet myDevReplSet

To initialize your server as a replica set, run the following command in
the mongo shell::

  rs.initiate()

Once the replica set is running, you may start mongo-connector. The
simplest invocation resembles the following::

  mongo-connector -m <mongodb server hostname>:<replica set port> \
                  -t <replication endpoint URL, e.g. http://localhost:8983/solr> \
                  -d <name of doc manager, e.g., solr_doc_manager>

mongo-connector has many other options besides those demonstrated above.
To get a full listing with descriptions, try ``mongo-connector --help``.
You can also use mongo-connector with a `configuration file <https://github.com/10gen-labs/mongo-connector/wiki/Configuration-File>`__.

Usage With Algolia
------------------

The simplest way to synchronize a collection ``myData`` from db ``myDb`` to index ``MyIndex`` is::

  mongo-connector -m localhost:27017 -n myDb.myCollection -d algolia_doc_manager -t MyApplicationID:MyApiKey:MyIndex

**Note**: If you synchronize multiple collections with multiple indexes, do not forget to specify a specific connector configuration file for each index using the ``-o config.txt`` option (a config.txt file is created by default).

Attributes remapping
~~~~~~~~~~~~~~~~~~~~

If you want to map an attribute to a specific index field, you can configure it creating a 
``algolia_remap_<INDEXNAME>.json`` JSON configuration file at the root of the mongo-connector folder::

  {
    "user.email": "email"
  }

Alternatively, you can use python-style subscript notation::

  {
    "['user']['email']": "['email']"
  }

**Note**:

- The remapping operation will run first.

Example
"""""""

Consider the following object::

  {
    "user": { "email": "my@algolia.com" }
  }

The connector will send::

  {
    "email": "my@algolia.com"
  }

Attributes filtering
~~~~~~~~~~~~~~~~~~~~

You can filter the attributes sent to Algolia creating a ``algolia_fields_INDEXNAME.json`` JSON configuration file::

  {
    "<ATTRIBUTE1_NAME>":"_$ < 0",
    "<ATTRIBUTE2_NAME>": ""
  }

Considering the following object::

  {
    "<ATTRIBUTE1_NAME>" : 1,
    "<ATTRIBUTE2_NAME>" : 2
  }

The connector will send::

  {
    "<ATTRIBUTE2_NAME>" : 2,
  }


**Note**: 

- ``_$`` represents the value of the field.
- An empty value for the check of a field is ``True``.
- You can put any line of python in the value of a field.
- The filtering operation will run between remapping and post-processing.

Filter an array attribute sent to Algolia
"""""""""""""""""""""""""""""""""""""""""

To select all elements from attribute ``<ARRARRAY_ATTRIBUTE_NAME>`` matching a specific condition::

  {
    "<ARRAY_ATTRIBUTE_NAME>": "re.match(r'algolia', _$, re.I)"
  }

Considering the following object::

  {
    "<ARRAY_ATTRIBUTE_NAME>" : ["algolia", "AlGoLiA", "alogia"]
  }

The connector will send::

  {
    "<ARRAY_ATTRIBUTE_NAME>": ["algolia", "AlGoLia"]
  }
    
Filter an object attribute in an array sent to Algolia
""""""""""""""""""""""""""""""""""""""""""""""""""""""

To select all elements from attribute ``status`` matching a specific condition::

  {
    "status": { "action": "", "outdated" : "_$ == false" }
  }

Considering the following object::

  {
    "status" : [
      {"action": "send", "outdated": "true"},
      {"action": "in transit", "outdated": true},
      {"action": "receive", "outdated": false}
    ]
  }

The connector will send::

  {
    "status": [{"action": "receive", "outdated": false}]
  }

Advanced nested objects filtering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to send a ``<ATTRIBUTE_NAME>`` attribute matching advanced filtering conditions, you can use::

  {
    "<ATTRIBUTE_NAME>": { "_all_" : "or", "neg": "_$ < 0", "pos": "_$ > 0"}
  }

Considering the following object::

  {
    "<ATTRIBUTE_NAME>": { "neg": 42, "pos": 42}
  }

The connector will send::

  {
    "<ATTRIBUTE_NAME>": { "pos": 42}
  }

Post processing
~~~~~~~~~~~~~~~

You can modify the attributes sent to Algolia creating a ``algolia_postproc_INDEXNAME.py`` Python script file::

  if (_$.get("<ATTRIBUTE_NAME>") == 0):
      _$["<ATTRIBUTE_NAME>"] = false
  else:
      _$["<ATTRIBUTE_NAME>"] = true
        
**Note**: 

- ``_$`` represents the record.
- The post-processing operation will run last.

Considering the following object::

  {
      "<ATTRIBUTE_NAME>": 0
  }
    
The connector will send::

  {
      "<ATTRIBUTE_NAME>": false
  }


Usage With Solr
---------------

There is an example Solr schema called
`schema.xml <https://github.com/10gen-labs/mongo-connector/blob/master/mongo_connector/doc_managers/schema.xml>`__,
which provides several field definitions on which mongo-connector
relies, including:

-  ``_id``, the default unique key for documents in MongoDB (this may be
   changed with the ``--unique-key`` option)
-  ``ns``, the namespace from which the document came
-  ``_ts``, the timestamp from the oplog entry that last modified the
   document

The sample XML schema is designed to work with the tests. For a more
complete guide to adding fields, review the `Solr
documentation <http://wiki.apache.org/solr/SchemaXml>`__.

You may also want to jump to the mongo-connector `Solr
wiki <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20Solr>`__
for more detailed information on using mongo-connector with Solr.

Troubleshooting
---------------

**Installation**

Some users have experienced trouble installing mongo-connector, noting
error messages like the following::

  Processing elasticsearch-0.4.4.tar.gz
  Running elasticsearch-0.4.4/setup.py -q bdist_egg --dist-dir /tmp/easy_install-gg9U5p/elasticsearch-0.4.4/egg-dist-tmp-vajGnd
  error: /tmp/easy_install-gg9U5p/elasticsearch-0.4.4/README.rst: No such file or directory

The workaround for this is making sure you have a recent version of
``setuptools`` installed. Any version *after* 0.6.26 should do the
trick::

  pip install --upgrade setuptools

**Running mongo-connector after a long time**

If you want to jump-start into using mongo-connector with a another particular system, check out:

- `Usage with Solr <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20Solr>`__
- `Usage with Elasticsearch <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20ElasticSearch>`__
- `Usage with MongoDB <https://github.com/10gen-labs/mongo-connector/wiki/Usage%20with%20MongoDB>`__

Troubleshooting/Questions
-------------------------

Having trouble with installation? Have a question about Mongo Connector?
Your question or problem may be answered in the `FAQ <https://github.com/10gen-labs/mongo-connector/wiki/FAQ>`__ or in the `wiki <https://github.com/10gen-labs/mongo-connector/wiki>`__.
If you can't find the answer to your question or problem there, feel free to `open an issue <https://github.com/10gen-labs/mongo-connector/issues>`__ on Mongo Connector's Github page.

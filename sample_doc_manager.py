"""Receives documents from the oplog worker threads and indexes them
    into the backend.

    This file is a starting point for a doc manager. The intent is
    that this file can be used as an example to add on different backends.
    To extend this to other systems, simply implement the exact same class and
    replace the method definitions with API calls for the desired backend.
    Each method is detailed to describe the desired behavior.
"""


class DocManager():
    """The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that
    multiple updates to the same doc reflect the most up to date version as
    opposed to
    multiple, slightly different versions of a doc.

    We are using elastic native fields for _id and ns, but we also store
    them as fields in the document, due to compatibility issues.
        """

    def __init__(self, url, auto_commit=True):
        """Verify Elastic URL and establish a connection.

        This method may vary from implementation to implementation, but it must
        verify the url to the backend and return None if that fails. It must
        also create the connection to the backend, and start a periodic
        committer if necessary. The Elastic uniqueKey is '_id', but this may be
        overridden
        by user defined configuration.
        """

    def upsert(self, doc):
        """Update or insert a document into Elastic

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """

    def remove(self, doc):
        """Removes documents from Elastic

        The input is a python dictionary that represents a mongo document.
        """

    def search(self, start_ts, end_ts):
        """Called to query Elastic for documents in a time range.

        This method is only used by rollbacks to query all the documents in
        Elastic within a certain timestamp window. The input will be two longs
        (converted from Bson timestamp) which specify the time range. The
        return value should be an iterable set of documents.
        """

    def commit(self):
        """This function is used to force a refresh/commit.

        It is used only in the beginning of rollbacks and in test cases, and is
        not meant to be called in other circumstances. The body should commit
        all documents to the backend engine (like auto_commit), but not have
        any timers or run itself again (unlike auto_commit). In the event of
        too many Elastic searchers, the commit is wrapped in a retry_until_ok
        to keep trying until the commit goes through.
        """

    def run_auto_commit(self):
        """Periodically commits to the Elastic server.

        This function commits all changes to the Elastic engine, and then
        starts a
        timer that calls this function again in one second. The reason for this
        function is to prevent overloading Elastic from other searchers. This
        function may be modified based on the backend engine and how commits
        are handled, as timers may not be necessary in all instances.
        """

    def get_last_doc(self):
        """Returns the last document stored in the Elastic engine.

        This method is used for rollbacks to establish the rollback window,
        which is the gap between the last document on a mongo shard and the
        last document in Elastic. If there are no documents, this functions
        returns None. Otherwise, it returns the first document.
        """
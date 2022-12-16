from pyArango.connection import *

from prominence.utilities import config

class Database(object):
    """
    Class for interacting with the database
    """
    def __init__(self):
        self._conn = Connection(username=config().get('database', 'username'),
                                password=config().get('database', 'password'))
        self._db = self._conn[config().get('database', 'database')]
        self._jobs = self._db["jobs"]

    def create_job(self, job):
        """
        Create job
        """
        doc = self._jobs.createDocument(initDict=job)
        doc._key = job['id']
        doc.save()

    def list_jobs(self):
        """
        List jobs
        """
        jobs = self._db.AQLQuery('FOR job IN jobs RETURN job', rawResults=True)
        jobs_list = []
        for job in jobs:
            jobs_list.append(job)
        return jobs_list

    def describe_job(self, id):
        """
        Describe job
        """
        job = self._jobs.fetchDocument(id, rawResults=True)
        return job

    def delete_job(self, id):
        """
        Delete job
        """
        job = self._jobs.fetchDocument(id)
        job['status'] = 'deleting'
        job.save()

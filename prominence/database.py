import time
from pyArango.connection import Connection

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
        self._workers = self._db["workers"]

    def create_job(self, job):
        """
        Create job
        """
        doc = self._jobs.createDocument(initDict=job)
        doc._key = job['id']
        doc.save()

    def list_jobs(self, status=None):
        """
        List jobs
        """
        filter = ''
        if status:
            filter = 'FILTER job.status == "%s"' % status
        jobs = self._db.AQLQuery(f"FOR job IN jobs {filter} RETURN job", rawResults=True)
        jobs_list = []
        for job in jobs:
            jobs_list.append(job)
        return jobs_list

    def get_job(self, id, rawResults=True):
        """
        Describe job
        """
        job = self._jobs.fetchDocument(id, rawResults=rawResults)
        return job

    def delete_job(self, id):
        """
        Delete job
        """
        job = self._jobs.fetchDocument(id)
        job['status'] = 'deleting'
        job['events'].append({'time': time.time(), 'type': 'deleting'})
        job.save()

    def get_pending_jobs(self):
        """
        Return pending jobs
        """
        jobs = self._db.AQLQuery('FOR job IN jobs FILTER job.status == "pending" RETURN job', rawResults=True)
        return jobs

    def update_status(self, id, status):
        """
        Update job status
        """
        job = self._jobs.fetchDocument(id)
        job['status'] = status
        job.save()

    def metrics(self):
        """
        Job metrics
        """
        running_jobs = self._db.AQLQuery('FOR job IN jobs FILTER job.status == "running" COLLECT WITH COUNT INTO length RETURN length', rawResults=True)[0]
        pending_jobs = self._db.AQLQuery('FOR job IN jobs FILTER job.status == "pending" COLLECT WITH COUNT INTO length RETURN length', rawResults=True)[0]

        return running_jobs, pending_jobs

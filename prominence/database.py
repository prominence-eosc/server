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

    def list_jobs(self):
        """
        List jobs
        """
        jobs = self._db.AQLQuery('FOR job IN jobs RETURN job', rawResults=True)
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
        job.save()

    def get_pending_jobs(self):
        """
        Return pending jobs
        """
        jobs = self._db.AQLQuery('FOR job IN jobs FILTER job.status == "pending" RETURN job', rawResults=True)
        return jobs

    def get_workers(self):
        """
        Return workers
        """
        workers = self._db.AQLQuery('FOR worker IN workers FILTER worker.resources.available.cpus > 0 AND worker.resources.available.memory > 0 AND worker.resources.available.disk > 0 RETURN worker', rawResults=True)
        return workers

    def get_lost_workers(self, threshold):
        """
        Return any lost workers
        """
        workers = self._db.AQLQuery('FOR worker IN workers FILTER %d - worker.epoch > %d RETURN worker' % (time.time(), threshold))
        return workers

    def delete_worker(self, doc):
        """
        Delete worker
        """
        doc.delete()

    def add_or_update_worker(self, data):
        """
        Add or update a worker
        """
        doc = self._workers.createDocument(initDict=data)
        doc._key = data['name']
        try:
            doc.save()
        except Exception as err:
            if 'unique constraint violated' in str(err):
                doc = self._workers[data['name']]
                doc['epoch'] = data['epoch']
                doc['jobs'] = data['jobs']
                doc['resources'] = data['resources']
                doc.save()

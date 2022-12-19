Next-generation PROMINENCE server - proof of concept using NATS as the basis for multi-cloud HTC/HPC. All communication between workers and the server is via NATS messaging, including:
* Workers advertising their resources & capabilities,
* Assignment of jobs to workers,
* Job status updates (e.g. running, completed),
* Job standard output/error

![overview](https://github.com/prominence-eosc/server/raw/main/overview.png)

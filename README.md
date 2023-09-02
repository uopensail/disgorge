# Swallow & Disgorge

## Purpose
In recommender system, we often face the problem of how to deal with bad cases and analyze them. Without proper logging, we would not be able to analyze. Therefore, the first thing is to keep these logs. However, keeping a detailed log can lead to excessive data volume, so we need to adopt an efficient way to record it. In addition, since the cost of logging can be high, we need to find a low-cost way to store it. Finally, since bad cases are not a frequent occurrence, we need to solve the query problem in a low-cost way. Our goal is to solve this problem in a low-cost way.

## Design
We split the store and query into two projects: Swallow and Disgorge. Swallow is used to store logs, Disgorge is used to query logs, and implements storage and computing separation.

1. Swallow

Logs are rotated hourly and written to network disks for organization and archiving. The use of network disks enables the entire architecture to have the following advantages:

- Large capacity: Network disks can provide large capacity storage, which can well adapt to the storage needs of large amounts of data.

- Low cost: Network disks offer lower costs and more flexibility in the choice of storage media than other storage media.

Rocksdb is configured to ensure data reliability and performance. When a time-slice rotation occurs, the previous Rocksdb will no longer accept new writes. However, we will compact it to improve the performance of later reading logs in openforreadonly mode.

1. Disgorge

Queries are made through DSL definitions, and data can be queried based on time ranges. Supported file opening methods include OpenAsSecondary and OpenForreadOnly.

- Openassecondary: opens the file as read-only, if there are other processes writing, it will wait for the write to be read. It is suitable for scenarios where a large number of data is not required in real time.

- Openforreadonly: Open the file as a shared read/write mode to read the latest content, and other processes can also read and write. It is suitable for scenarios that require high real-time data.

When the log is being written, or when a time slice rotation has been performed but not yet compacted, and a query service comes in, we will open Rocksdb using openassecondary. For other cases, we open it in openforreadonly mode. In addition, we reopen the Rocksdb file independently each time we query and close it again after the query is over.

## Deployment

1. Docker image

Using Docker images for deployment makes it easy to deploy and manage services, and can adapt to different environmental requirements.

2. Terraform deployment

Deploying with Terraform automates deployment and resource management, greatly improving deployment efficiency.

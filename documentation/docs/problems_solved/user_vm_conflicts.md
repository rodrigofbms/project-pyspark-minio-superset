# Docker Permission Conflicts Between Containers and Linux VM

## Overview

During the development of a containerized data environment using **Apache Spark**, **Jupyter Notebook**, **Docker**, and **Linux**, an important infrastructure issue was identified involving permission conflicts between Docker containers and the host VM.

The problem directly affected:

- file creation
- mounted volumes
- read/write operations
- environment consistency
- pipeline reliability

This document explains the root cause, the temporary fixes tested, and the definitive architectural solution adopted.

---

## Problem Identified

Initially, Apache Spark and Jupyter Notebook were running inside the same Docker container.

Over time, several inconsistent behaviors started to appear:

- files were not being created correctly
- directories had unexpected permission restrictions
- mounted volumes behaved inconsistently
- some operations failed without clear error messages

After investigation, the root cause was identified:

> The container was running with a different user than the Linux VM host.

Example:

- Container user: `uid=0` (`root`)
- Linux VM user: `uid=1000`

Since Docker mounted volumes inherit filesystem permissions from the host machine, this mismatch generated direct read/write permission conflicts.

---

## Initial Temporary Fixes

Some common Linux permission commands were initially tested:

```bash
chmod -R 777 /my/directory
chown -R user:user /my/directory
```
Or even:

```bash
chown -R 1000:1000 /my/directory
```
Although these approaches solved the issue temporarily, the problem returned depending on the container execution context.

At this point, it became clear that:

The issue was not only permissions — it was environment architecture.

## Architectural Solution

### 1. Container Responsibility Separation

Before:
- One single container running:
- Apache Spark
- Jupyter Notebook

After:
- Dedicated Spark container
- Dedicated Jupyter container

This change provided:
- process isolation
- better environment organization
- reduced permission conflicts
- improved maintainability
- cleaner infrastructure design


### 2. User Synchronization Between Container and VM

The definitive solution was ensuring that the container used the same UID and GID as the Linux host machine.

```bash
services:
  jupyter:
    image: jupyter/pyspark-notebook
    user: "root"

    environment:
      NB_UID=${VM_UID}
      NB_GID=${VM_GID}
      CHOWN_HOME=yes
      CHOWN_HOME_OPTS=-R

    volumes:
      - ./data:/home/jovyan/work
```

### 3.Linux Host Configuration

```bash
export VM_UID=$(id -u)
export VM_GID=$(id -g)
```

This synchronization ensured that:

- containers operated using the same user identifiers as the VM
- mounted volumes maintained consistent ownership
- file creation became predictable
- read/write conflicts were eliminated


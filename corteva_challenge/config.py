#!/usr/bin/env python3
# coding: utf-8
import os

# Data source GitHub repository details
DATA_SRC_GITHUB_REPO_NAME = "code-challenge-template"
DATA_SRC_GITHUB_REPO_OWNER = "corteva"

# PostgreSQLAlchemy DB details
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
#!/bin/bash

dbt clean
dbt deps
dbt compile

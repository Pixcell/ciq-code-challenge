# Introduction

The intention of this coding challenge is to gauge your ability to program, write clean code, and discuss your work with others. Our expectation is to receive a well written solution with a clear approach that could potentially have room for improvement. With this in mind, we recommend spending no more than an hour or two on this challenge.

Some important considerations for your implementation:

* Adherence to language standards
* Well organized & documented code
* Efficient time & space complexity
* Reusable and/or extensible code

# Scenario

You are given a small sample of log of events(`server_log.csv`) for a server that allows users to upload or download files.

Using this log file as example input, write an interactive program in Go that will allow a server administrator to gather metrics from server logs in this format.

Your solution must be able to answer the following types or combinations of questions about the example log:

1. How many users accessed the server?
2. How many uploads were larger than `50kB`?
3. How many times did `jeff22` upload to the server on April 15th, 2020?

Please include a small `README.md` describing your solution and additional considerations you would make if you had more time.

# Details

The log is represented by a CSV formatted file where column data is ordered: `timestamp`, `username`, `operation`, `size`.

- The `timestamp` is recorded in the UNIX date format
- The `username` is a unique identifier for the user
- The `operation` indicates if an `upload` or `download` occurred, no other values will appear in this column
- The `size` is an integer reflecting file size in `kB`
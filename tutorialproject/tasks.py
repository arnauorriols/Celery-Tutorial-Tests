from __future__ import absolute_import

from tutorialproject.celery import app

@app.task
def add(x, y):
    return x + y

@app.task
def multiply(x, y):
    return x * y

@app.task
def xsum(numbers):
    return sum(numbers)



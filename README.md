[![codecov](https://codecov.io/gh/takeru911/faws/branch/master/graph/badge.svg)](https://codecov.io/gh/takeru911/faws)

# fake aws

fakeaws(faws) is a service that mimics each of the aws services.
This is my private development work, so you should not use it in production.
Here's what's currently being faked
  - SQS


## development

- install

```
$ poetry install
```

- run server

```
$ make run/${SERVICE}
# for run sqs
$ make run/sqs
```

- call by awscli

```
$ aws sqs create-queue --queue-name test --endpoint http://localhost:5000
```

### testing

```
$ make test/all
```

If you want to run only a specific service, you can do so as follows

```
$ make test/sqs
```

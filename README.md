## Setup
* Install [Gordon](https://gordon.readthedocs.io/en/latest/index.html])
* Install [Docker](https://www.docker.com/community-edition)
* I think you need to make sure that you've got AWS credentials setup in $HOME/.aws (instructions [here](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files))

## Run unit tests
### Start local DynamoDB instance
`./bin/start-dynamodb`

### Run unit tests
* `./bin/run-unit-tests`

### Stop local DynamoDB instance (after you're all done)
`./bin/stop-dynamodb`



## Run integration tests
### Create remote DynamoDB table
`T_STAGE=prod ./bin/create-table`

### Deploy latest code
`./bin/do-deploy`

### Modify API gateway endpoint
After you deploy, Gordon will spit back a set of outputs like:
```
Project Outputs:
  ApigatewayHelloworldapi
    https://rgx1pkb8se.execute-api.us-west-2.amazonaws.com/dev
```

Replace the `API_GATEWAY_URL` in `bin/run-integration-tests` with the value that Gordan returns (`https://rgx1pkb8se.execute-api.us-west-2.amazonaws.com/dev` in this case)

NOTE: You should only have to do this the first time you deploy

### Run integration tests
`T_STAGE=prod ./bin/run-integration-tests`

### Delete table (after you're all done)
`T_STAGE=prod ./bin/delete-table`

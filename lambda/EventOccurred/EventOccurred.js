'use strict';
var aws = require('aws-sdk');
var ddbm = require('dynamodb-marshaler');

var sns = new aws.SNS({ apiVersion: '2010-03-31' });
var db = new aws.DynamoDB({ apiVersion: '2012-08-10' });

function m(onFail) {
  var withFail = (function (onSuccess) {
    return (function (err, data) {
      if (err) {
        withFail.fail(err);
      } else {
        onSuccess(data);
      }
    });
  });

  withFail.fail = function fail(err) {
    console.log(err);
    onFail(err);
  };

  return withFail;
}

function mForDb(cb) {
  return m(function (err) { cb('Error accessing database.'); });
}

function getUser(key, cb) {
  console.log('Getting user');
  var l = mForDb(cb);
  db.query({
    TableName: 'ifttt_glue_users',
    IndexName: 'key-index',
    ProjectionExpression: 'id',
    Select: 'SPECIFIC_ATTRIBUTES',
    KeyConditionExpression: '#key_name = :key_value',
    ExpressionAttributeNames: {
      '#key_name': 'key'
    },
    ExpressionAttributeValues: {
      ':key_value': { 'S': key }
    }
  }, l(function (data) {
    if (data.Count != 1) {
      cb('Not authorized.');
    } else {
      cb(null, ddbm.unmarshalItem(data.Items[0]));
    }
  }));
}

function publishMessage(user, message, cb) {
  console.log('Publishing to SNS');
  var l = m(function (err) {
    console.log('Error publishing to SNS:', err);
    cb('Error publishing to SNS.');
  });
  message.userId = user.id;
  sns.publish({
    Message: JSON.stringify(message),
    TopicArn: 'arn:aws:sns:us-east-1:547160500625:ifttt_glue_event'
  }, l(function (res) {
    cb(null, res);
  }));
}

function getOrFail(event, context, field) {
  var value = event[field];
  if (value == null) {
    var message = 'Invalid value provided for "' + field + '": parameter is required.';
    context.fail(message);
  } else {
    return value;
  }
}

exports.handler = function (event, context) {
  console.log('Received event:' + JSON.stringify(event));
  var message = {};
  var key = getOrFail(event, context, 'key');
  message.action = getOrFail(event, context, 'action');
  if (key == null || message.action == null) {
    return;
  } else if ([ 'add', 'append', 'divide', 'enqueue', 'multiply', 'set', 'subtract' ].indexOf(message.action) >= 0) {
    message.value = getOrFail(event, context, 'value');
  } else if ([ 'clear', 'decrement', 'increment', 'toggle' ].indexOf(message.action) < 0) {
    context.fail('Invalid value provided for "action": unrecognized action "' + message.action + '"');
    return;
  }

  var l = m(function (err) {
    console.log('An error occurred:', err);
    context.fail(err);
  });
  getUser(key, l(function (user) {
    console.log('Got user:', user);
    publishMessage(user, message, l(function (res) {
      console.log('Published to SNS:', JSON.stringify(res));
      context.succeed({});
    }));
  }));
};

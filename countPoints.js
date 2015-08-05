var bluebird = require('bluebird');
var _ = require('lodash');
var csv = require('csv-parser');
var express = require('express');

var fs = bluebird.promisifyAll(require('fs'));
var glob = bluebird.promisify(require('glob'));

var Promise = bluebird;

function createCsvStream() {
  return csv({strict: true});
}

function createCsvPromise(csvPath, boundObject, onData, onError) {
  return new Promise(function(fulfill, reject) {
    var originStream = fs.createReadStream(csvPath).pipe(createCsvStream())
    .on('data', onData.bind(boundObject))
    .on('end', function() {
      fulfill(boundObject);
    });

    if (onError) {
      originStream.on('error', function(err) {
        onError.call(boundObject, err);
        fulfill(boundObject);
      });
    }
    else {
      originStream.on('error', function(err) {
        reject(err);
      });
    }
  });
}

// Find participants
glob(submissionsDir + '/*')

// Find participants
.then(function(files) {
  var participantPromises = _.map(files, function(participantPath) {
    var parts = participantPath.split('/');
    var participantObj = {
      name: parts[parts.length-1],
      path: participantPath,
      indexPath: participantPath + '/index.csv',
      csvDir: participantPath + '/csv',
      submissions: []
    };

    var participantPromise =
      createCsvPromise(participantObj.indexPath, participantObj, function(row) {
        row.resultPath =
          fs.realpathSync(this.csvDir + '/' + row.result_filename);
        this.submissions.push(row);
      });

    return participantPromise;
  });

  return Promise.all(participantPromises);
})

.then(function(participants) {
  var participantPromises = _.map(participants, function(participantObj) {
    var submissionPromises =
      _.map(participantObj.submissions, function(submission) {
        submission.cellCount = 0;
        submission.isValid = true;
        submission.validationError = '';

        var colCount = null;

        var submissionPromise =
          createCsvPromise(submission.resultPath, submission, function(row) {
            if (!colCount) {
              colCount = Object.keys(row).length;
            }

            submission.cellCount += colCount;
          }, function(err) {
            submission.isValid = false;
            submission.validationError = err.message;
          });

        return submissionPromise;
      });

    return Promise.all(submissionPromises).then(function() {
      return participantObj;
    });
  });

  return Promise.all(participantPromises);
})

.then(function(participants) {
  console.dir(participants, { depth: 3, colors: true });
}, function(err) {
  throw err;
});
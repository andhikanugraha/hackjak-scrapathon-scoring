var bluebird = require('bluebird');
var _ = require('lodash');
var csv = require('csv-parser');
var stream = require('stream');
var express = require('express');
var open = require('open');
var winston = require('winston');

var fs = bluebird.promisifyAll(require('fs'));
var glob = bluebird.promisify(require('glob'));

var argv = require('minimist')(process.argv.slice(2));
var Promise = bluebird;

var logger = winston;
winston.cli();

var submissionsRelativePath = argv._[0] || '../submissions';
var submissionsPath = '';

var serverPort = argv.port || argv.p || process.env.PORT || 3000;

var allParticipants = [];
var sortedParticipants = [];
var participantsByName = {};

var sampleSize = 0.1;

function randomIntegerBetween(i, j) {
  var delta = j - i;
  var rand = Math.round(Math.random() * delta);
  return Math.round(rand) + i;
}

function createCsvStream() {
  return csv({strict: true});
}

logger.info('HackJak Scrapathon Scoring Tool');
logger.info('');

logger.info('Resolving submissions directory: ' + submissionsRelativePath);
fs.realpathAsync(submissionsRelativePath)
.then(function(submissionsDir) {
  submissionsPath = submissionsDir;
  logger.info('Listing folders under ' + submissionsPath + '...');
  return glob(submissionsDir + '/*');
})
.then(function(files) { // List participants
  logger.info('  ' + files.length + ' folders found.');
  var participantPromises = _.map(files, function(participantPath) {
    var parts = participantPath.split('/');
    var participantObj = {
      name: parts[parts.length-1],
      path: participantPath,
      indexPath: participantPath + '/index.csv',
      csvDir: participantPath + '/csv',
      submissions: []
    };

    var indexCsvStream =
      fs.createReadStream(participantObj.indexPath).pipe(csv());

    indexCsvStream.on('data', function(row) {
      if (row.result_filename) {
        row.resultPath = participantObj.csvDir + '/' + row.result_filename;
        participantObj.submissions.push(row);
      }
    });
    indexCsvStream.on('error', function(err) {
      logger.warn(err);
    });

    var participantPromise = new Promise(function(fulfill, reject) {
      indexCsvStream.on('end', function() {
        fulfill(participantObj);
      });
    });

    return participantPromise;
  });

  return Promise.all(participantPromises);
})
.then(function(participants) { // Count cells
  logger.info('Counting participant points...');
  var participantPromises = _.map(participants, function(participantObj) {
    participantObj.totalPoints = 0;
    participantObj.validCsvCount = 0;
    participantObj.invalidCsvCount = 0;

    var submissions = participantObj.submissions;
    var submissionPromises =
      _.map(participantObj.submissions, function(submission) {
        submission.cellCount = 0;
        submission.isValid = true;
        submission.validationError = null;

        var colCount = null;

        var csvStream =
          fs.createReadStream(submission.resultPath).pipe(createCsvStream());

        csvStream.on('data', function(row) {
          if (!colCount) {
            colCount = Object.keys(row).length;
          }

          submission.cellCount += colCount;
        });

        var submissionPromise = new Promise(function(fulfill, reject) {
          csvStream.on('error', function(err) {
            submission.isValid = false;
            submission.validationError = err.message;
            csvStream.removeAllListeners('data');
            fulfill(submission);
          });
          csvStream.on('end', function() {
            fulfill(submission);
          });
        });

        return submissionPromise;
      });

    return Promise.all(submissionPromises).then(function() {
      _.each(submissions, function(submission) {
        if (submission.isValid) {
          participantObj.totalPoints += submission.cellCount;
          ++participantObj.validCsvCount;
        }
        else {
          ++participantObj.invalidCsvCount;
        }
      });
           
      return participantObj;
    });
  });

  return Promise.all(participantPromises);
})
.then(function(participants) {
  allParticipants = participants;
  _.each(participants, function(participant) {
    logger.info('');
    logger.info('Participant: ' + participant.name);

    participantsByName[participant.name] = participant;

    logger.info('Sorting submissions...');
    participant.sortedSubmissions =
      _.sortByOrder(participant.submissions,
        ['isValid', 'cellCount'], ['desc', 'desc']);

    // Generate stratified random samples
    logger.info('Electing stratified random samples...');

    // Just the validated submissions
    var cleanSubmissions = _.filter(participant.sortedSubmissions, 'isValid');
    logger.info('  Number of valid submissions: ' + cleanSubmissions.length);

    // The number of samples to take from this participant's submissions
    var sampleCount = Math.ceil(sampleSize * cleanSubmissions.length);
    logger.info('  Number of samples: ' + sampleCount);

    // The number of submissions per bracket/strata
    var bracketSize = Math.ceil(cleanSubmissions.length / sampleCount);
    logger.info('  Submissions per strata: ' + bracketSize);

    participant.samples = [];
    for (var i = 0; i < cleanSubmissions.length; i += bracketSize) {
      var j = Math.min(i + bracketSize - 1, cleanSubmissions.length - 1);
      var sampleKey = randomIntegerBetween(i, j);
      participant.samples.push(cleanSubmissions[sampleKey]);
    }
  });

  logger.info('');
  logger.info('Sorting participants...');
  sortedParticipants = _.sortByOrder(participants, ['totalPoints'], ['desc']);
  _.each(sortedParticipants, function(participant, key) {
    participant.rank = key + 1;
  });

  var app = express();
  app.use(express.static('public'));
  app.set('view engine', 'hbs');
  app.get('/', function(req, res) {
    res.locals.sortedParticipants = sortedParticipants;
    res.render('index');
  });
  app.get('/details/:name', function(req, res) {
    var participant = participantsByName[req.params.name];

    // console.dir(participant);
    res.locals.participant = participant;
    res.locals.samples = participant.samples;

    console.dir(participant.samples, { colors: true });
    res.render('participant');
  });
  app.get(/\/details\/([^\/]+)\/(.+\.csv)/, function(req, res, next) {
    var nameParam = req.params[0];
    var pathParam = req.params[1];
    if (!participantsByName[nameParam]) {
      return next();
    }

    var csvPath = submissionsPath + '/' + nameParam + '/csv/' + pathParam;
    if (!fs.existsSync(csvPath)) {
      return next();
    }

    var rows = [];
    var body = '';
    var keys;
    fs.createReadStream(csvPath).pipe(csv()).on('data', function(row) {
      if (!keys) {
        keys = Object.keys(row);
      }
      body += '<tr><td>' + _.toArray(row).join('</td><td>') + '</td></tr>\n';
    }).on('end', function() {
      res.locals.name = nameParam;
      res.locals.path = pathParam;
      res.locals.rows = rows;
      res.locals.keys = keys;
      res.locals.tableBody = body;
      res.render('csv');
    }).on('error', function() {});
  });

  logger.info('');
  logger.info('Starting Express...');
  app.listen(serverPort, function() {
    logger.info('Express listening on port ' + serverPort + '.');
    open('http://localhost:' + serverPort);
  });
})
.catch(function(err) {
  logger.error(err);
});
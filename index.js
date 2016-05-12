'use strict';

var exec = require('child_process').exec
    , spawn = require('child_process').spawn
    , path = require('path')
    , domain = require('domain')
    , d = domain.create();


/**
 * log
 *
 * Logs a message to the console with a tag.
 *
 * @param message  the message to log
 * @param tag      (optional) the tag to log with.
 */
function log(message, tag) {
  var util = require('util')
      , color = require('cli-color')
      , tags, currentTag;

  tag = tag || 'info';

  tags = {
    error: color.red.bold,
    warn: color.yellow,
    info: color.cyanBright
  };

  currentTag = tags[tag] || function(str) { return str; };
  util.log((currentTag("[" + tag + "] ") + message).replace(/(\n|\r|\r\n)$/, ''));
}

/**
 * getArchiveName
 *
 * Returns the archive name in database_YYYY_MM_DD.tar.gz format.
 *
 * @param databaseName   The name of the database
 */
function getArchiveName(databaseName) {
  var date = new Date()
      , datestring;

  datestring = [
    databaseName,
    date.getFullYear(),
    date.getMonth() + 1,
    date.getDate(),
    date.getTime()
  ];

  return datestring.join('_') + '.tar.gz';
}

/* removeRF
 *
 * Remove a file or directory. (Recursive, forced)
 * Remove only  7 day or older files
 *
 * @param target       path to the file or directory
 * @param callback     callback(error)
 */
function removeRF(target, callback) {
  var fs = require('fs');
  callback = callback || function(res) { console.log(res) };

    fs.exists(target, function(exists) {
    if (!exists) {
      return callback(null);
    } else{
            log("Removing " + target, 'info');
            exec('rm -rf ' + target, callback);
        }
  });
}
/* removeOldArchives
 * Removes older archives older than a certain time
 * @param target       path to the archive directory
 * @param callback     callback(error)
 */
function removeOldArchives(target, callback) {

    var fs = require('fs');
    callback = callback || function(res) { console.log(res) };

    fs.exists(target, function (exists) {
        if (!exists) {
            return callback(null);
        }
        else
        {
            var files = fs.readdirSync(target);
            files.forEach(function (file) {
                var filePath = path.join(target, file);
                var stat = fs.statSync(filePath);
                if (stat.isFile()) {
                    var filename = path.basename(filePath);
                    var dateParts = filename.substring(5, 14).split("_");
                    var backupDate = new Date(dateParts[0], (dateParts[1] - 1), dateParts[2]);
                    var thresholdDate = new Date();
                    thresholdDate.setDate(thresholdDate.getDate() - 5);
                    //delete files older than  7 days
                    if (backupDate < thresholdDate) {
                        log("Removing " + filePath, 'info');
                        exec('rm -rf ' + filePath, callback);
                    }
                    else{
                        console.log('No Archives Deleted')
                    }
                }
                else {
                    log("Removing " + target, 'info');
                    exec('rm -rf ' + target, callback);
                }
            });
            callback()
        }
    });
}

/**
 * mongoDump
 *
 * Calls mongodump on a specified database.
 *
 * @param options    MongoDB connection options [host, port, username, password, db]
 * @param directory  Directory to dump the database to
 * @param callback   callback(err)
 */
function mongoDump(options, directory, callback) {
  var mongodump
      , mongoOptions;

  callback = callback || function() { };

  mongoOptions= [
    '-h', options.host + ':' + options.port,
    '-d', options.db,
    '-o', directory
  ];

  if(options.username && options.password) {
    mongoOptions.push('-u');
    mongoOptions.push(options.username);

    mongoOptions.push('-p');
    mongoOptions.push(options.password);
  }

  log('Starting mongodump of ' + options.db, 'info');
  mongodump = spawn('mongodump', mongoOptions);

  mongodump.stdout.on('data', function (data) {
    log(data);
  });

  mongodump.stderr.on('data', function (data) {
    log(data, 'error');
  });

  mongodump.on('exit', function (code) {
    if(code === 0) {
      log('mongodump executed successfully', 'info');
      callback(null);
    } else {
      callback(new Error("Mongodump exited with code " + code));
    }
  });
}

/**
 * compressDirectory
 *
 * Compressed the directory so we can upload it to S3.
 *
 * @param directory  current working directory
 * @param input     path to input file or directory
 * @param output     path to output archive
 * @param callback   callback(err)
 */
function compressDirectory(directory, input, output, callback) {

  var tar;
  var tarOptions;

  callback = callback || function() { };

  tarOptions = [
    '-zcf',
    output,
    input
  ];

  log('Starting compression of ' + input + ' into ' + output, 'info');
  tar = spawn('tar', tarOptions, { cwd: directory });

  tar.stderr.on('data', function (data) {
    log(data, 'error');
  });

  tar.on('exit', function (code) {
    if(code === 0) {
      log('successfully compress directory', 'info');
      callback(null);
    } else {
      callback(new Error("Tar exited with code " + code));
    }
  });
}

/**
 * sendToS3
 * Implementation Pending to do multi part update
 * Sends a file or directory to S3.
 *
 * @param options   s3 options [key, secret, bucket]
 * @param directory directory containing file or directory to upload
 * @param target    file or directory to upload
 * @param callback  callback(err)
 */
function sendToS3(options, directory, target, callback) {
    var knox = require('knox')
        , sourceFile = path.join(directory, target)
        , s3client
        , destination = options.destination || '/'
        , headers = {};

    callback = callback || function() { };

    // Deleting destination because it's not an explicitly named knox option
    delete options.destination;
    s3client = knox.createClient(options);

    if (options.encrypt)
        headers = {"x-amz-server-side-encryption": "AES256"}

    log('Attemping to upload ' + target + ' to the ' + options.bucket + ' s3 bucket');
    s3client.putFile(sourceFile, path.join(destination, target), headers, function(err, res){
        if(err) {
            return callback(err);
        }

        res.setEncoding('utf8');

        res.on('data', function(chunk){
            if(res.statusCode !== 200) {
                log(chunk, 'error');
            } else {
                log(chunk);
            }
        });

        res.on('end', function(chunk) {
            if (res.statusCode !== 200) {
                return callback(new Error('Expected a 200 response from S3, got ' + res.statusCode));
            }
            log('Successfully uploaded to s3');
            return callback();
        });
    });

    multipart.uploadFile(sourceFile,function(result){

        console.log('value returned from s3 multi-part module upload',result);
    })

    //callback("Implementatioin Pending for S3 load")
}

/**
 * sync
 *
 * Performs a mongodump on a specified database, gzips the data,
 * and uploads it to s3.
 *
 * @param mongodbConfig   mongodb config [host, port, username, password, db]
 * @param s3Config        s3 config [key, secret, bucket]
 * @param callback        callback(err)
 */
function sync(config, s3Config, callback) {

    var tmpDir = config.backup.tmp
      , archiveDir = config.backup.archive
      , archiveName = path.join(config.backup.archive,getArchiveName(config.mongodb.db))
      , async = require('async');


   callback = callback || function() { };

    var steps = [
         async.apply(removeRF, tmpDir),
         async.apply(removeOldArchives, archiveDir),
         async.apply(mongoDump, config.mongodb, tmpDir),
         async.apply(compressDirectory, tmpDir,config.mongodb.db,archiveName)
        //d.bind(async.apply(sendToS3, s3Config, tmpDir, archiveName)) // this function sometimes throws EPIPE errors
    ];

    async.series(steps, function(err) {
      if (err) {
          log(err, 'error');
      } else {
          log('Successfully backed up ' + config.mongodb.db);
      }
    });

}

/*removeRF('/data/tmp/mongodb_s3_backup/',function(obj){
    console.log('ran the delete fn')
})*/

module.exports = { sync: sync, log: log };

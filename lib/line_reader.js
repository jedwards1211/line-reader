(function() {
  "use strict";

  var fs = require('fs'),
      StringDecoder = require('string_decoder').StringDecoder;

  function LineReader(fd, cb, separator, encoding, bufferSize) {
    var filePosition   = 0,
        encoding       = encoding || 'utf8',
        separator      = separator || '\n',
        bufferSize     = bufferSize || 1024,
        buffer         = new Buffer(bufferSize),
        bufferStr      = '',
        decoder        = new StringDecoder(encoding),
        closed         = false,
        eof            = false,
        separatorIndex = -1;

    function close() {
      if (!closed) {
        fs.close(fd, function(err) {
          if (err) {
            throw err;
          }
        });
        closed = true;
      }
    }

    function readToSeparator(cb) {
      function readChunk() {
        fs.read(fd, buffer, 0, bufferSize, filePosition, function(err, bytesRead) {
          var separatorAtEnd;

          if (err) {
            throw err;
          }

          if (bytesRead < bufferSize) {
            eof = true;
            close();
          }

          filePosition += bytesRead;

          bufferStr += decoder.write(buffer.slice(0, bytesRead));

          if (separatorIndex < 0) {
            separatorIndex = bufferStr.indexOf(separator);
          }

          separatorAtEnd = separatorIndex === bufferStr.length - 1;
          if (bytesRead && (separatorIndex === -1 || separatorAtEnd) && !eof) {
            readChunk();
          } else {
            cb();
          }
        });
      }

      readChunk();
    }

    function hasNextLine() {
      return bufferStr.length > 0 || !eof;
    }

    function nextLine(cb) {
      function getLine() {
        var ret = bufferStr.substring(0, separatorIndex);

        bufferStr = bufferStr.substring(separatorIndex + separator.length);
        separatorIndex = -1;
        cb(ret);
      }

      if (separatorIndex < 0) {
        separatorIndex = bufferStr.indexOf(separator);
      }

      if (separatorIndex < 0) {
        if (eof) {
          if (hasNextLine()) {
            separatorIndex = bufferStr.length;
            getLine();
          } else {
            throw new Error('No more lines to read.');
          }
        } else {
          readToSeparator(getLine);
        }
      } else {
        getLine();
      }
    }

    this.hasNextLine = hasNextLine;
    this.nextLine = nextLine;
    this.close = close;

    readToSeparator(cb);
  }

  function open(filename, cb, separator, encoding, bufferSize) {
    fs.open(filename, 'r', parseInt('666', 8), function(err, fd) {
      var reader;
      if (err) {
        throw err;
      }

      reader = new LineReader(fd, function() {
        cb(reader);
      }, separator, encoding, bufferSize);
    });
  }

  function eachLine(filename, cb, separator, encoding, bufferSize) {
    var finalFn,
        asyncCb = cb.length == 3;

    function finish() {
      if (finalFn && typeof finalFn === 'function') {
        finalFn();
      }
    }

    open(filename, function(reader) {
      function newRead() {
        if (reader.hasNextLine()) {
          setImmediate(readNext);
        } else {
          finish();
        }
      }

      function continueCb(continueReading) {
        if (continueReading !== false) {
          newRead();
        } else {
          finish();
          reader.close();
        }
      }

      function readNext() {
        reader.nextLine(function(line) {
          var last = !reader.hasNextLine();

          if (asyncCb) {
            cb(line, last, continueCb);
          } else {
            if (cb(line, last) !== false) {
              newRead();
            } else {
              finish();
              reader.close();
            }
          }
        });
      }

      newRead();
    }, separator, encoding, bufferSize);

    return {
      then: function(cb) {
        finalFn = cb;
      }
    };
  }

  module.exports.open = open;
  module.exports.eachLine = eachLine;

  function SyncLineReader(fd, options) {
    if (!options) options = {};

    var filePosition   = 0,
        encoding       = options.encoding || 'utf8',
        separator      = options.separator || /\r\n|\r|\n/,
        bufferSize     = options.bufferSize || 1024,
        buffer         = new Buffer(bufferSize),
        bufferStr      = '',
        decoder        = new StringDecoder(encoding),
        closed         = false,
        eof            = false,
        separatorIndex = -1,
        separatorLen   = -1;

    var findSeparator;

    if (separator instanceof RegExp) {
      findSeparator = function() {
        var result = separator.exec(bufferStr);
        if (result && (result.index + result[0].length < bufferStr.length) || eof) {
          separatorIndex = result.index;
          separatorLen = result[0].length;
        }
        else {
          separatorIndex = -1;
        }
      }
    }
    else {
      findSeparator = function() {
        separatorIndex = bufferStr.indexOf(separator);
      }
      separatorLen = separator.length;
    }

    function close() {
      if (!closed) {
        closed = true;
        fs.closeSync(fd);
      }
    }

    function readToSeparator() {
      function readChunk() {
        var bytesRead = fs.readSync(fd, buffer, 0, bufferSize, filePosition);

        if (bytesRead < bufferSize) {
          eof = true;
        }

        filePosition += bytesRead;

        bufferStr += decoder.write(buffer.slice(0, bytesRead));

        findSeparator();

        return bytesRead;
      }

      var bytesRead;
      do {
        bytesRead = readChunk();
      } while (bytesRead && separatorIndex < 0 && !eof);
    }

    function hasNextLine() {
      return bufferStr.length > 0 || !eof;
    }

    function nextLine() {
      if (separatorIndex < 0) {
        findSeparator();
      }

      while (separatorIndex < 0) {
        if (eof) {
          if (hasNextLine()) {
            separatorIndex = bufferStr.length;
            break;
          } else {
            throw new Error('No more lines to read.');
          }
        }
        readToSeparator();
      }
      var ret = bufferStr.substring(0, separatorIndex);

      bufferStr = bufferStr.substring(separatorIndex + separatorLen);
      separatorIndex = -1;
      return ret;
    }

    this.hasNextLine = hasNextLine;
    this.nextLine = nextLine;
    this.close = close;

    readToSeparator();
  }

  var openSync = module.exports.openSync = function(filename, options) {
    var fd = fs.openSync(filename, 'r', parseInt('666', 8));
    return new SyncLineReader(fd, options);
  }

  var eachLineSync = module.exports.eachLineSync = function(filename, options, cb) {
    if (options instanceof Function) {
      cb = options;
      options = undefined;
    }
    var lineReader = openSync(filename, options);
    try {
      while (lineReader.hasNextLine()) {
        var line = lineReader.nextLine();
        var last = !lineReader.hasNextLine();
        if (cb(line, last) === false) {
          return;
        }
      }
    }
    finally {
      lineReader.close();
    }
  }
}());

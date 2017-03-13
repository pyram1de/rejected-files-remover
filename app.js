var fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
var targetDir = 'e:\\temp\\',
    targetFile = 'files.txt',
    sourceDir = 'z:\\',
    newLine = '\r\n',
    url = 'mongodb://localhost:27017/image-rejects',
    auditFilePath = 'e:\\temp\\audit.log',
    auditLine = '';

run(function* myDelayedMessages(resume){
    var db = yield MongoClient.connect(url, resume);
    var rejectedcalcs = db.collection('rejectedcalcs');
    var files = fs.readdirSync(sourceDir);
    for(var i = 0;i<files.length;i++){                
        var stats = fs.statSync(sourceDir + files[i]);        
        if(stats.isDirectory()){
            auditLine = sourceDir + files[i];
            var calcDir = files[i];
            var calcDirFileDirectory = sourceDir+calcDir;
            // find the csv file from each folder and process them.                
            var calcDirFiles = fs.readdirSync(calcDirFileDirectory);
            for(var x = 0;x < calcDirFiles.length; x = x + 1){
                if(calcDirFiles[x].substr(-4)==='.csv'){
                    auditLine += '\t' + calcDirFiles[x];
                    var theCSVLocation = calcDirFiles[x];
                    var theCSVbyRow = fs.readFileSync(sourceDir+calcDir+'\\'+theCSVLocation, 'utf8').split(newLine);
                    var failedCount = 0,passedCount = 0, size = 0;
                    //identifier,filename,type,date                                        
                    while(theCSVbyRow.length>1){
                        var theRow = theCSVbyRow.shift();
                        var splitRows = theRow.split(',');
                        var data = yield rejectedcalcs.findOne(
                                {
                                    "Identifier":Number(splitRows[0], 10),
                                    "file":splitRows[1]
                                },resume);
                        if(data) {
                            var fileStat = fs.statSync(calcDirFileDirectory+'\\'+splitRows[1]);
                            size += fileStat.size / 1000 / 1000;
                            fs.appendFileSync(targetDir+'fail'+targetFile,theRow + '\r\n');                            
                            ++failedCount;
                        } else {
                            fs.appendFileSync(targetDir+targetFile,theRow + '\r\n');
                            ++passedCount;
                        }
                    }        
                    auditLine = auditLine + '\t' + passedCount+ '\t' + failedCount + '\t' + size;
                    fs.appendFileSync(auditFilePath, auditLine + '\r\n');
                }
            }
        }
    }
    db.close();
    console.log('done');
});

function run(generatorFunction){
    var generatorItr = generatorFunction(resume);    
    function resume(err, callbackValue){    
        if(err){
            generatorItr.next(null);    
        } else {
            generatorItr.next(callbackValue);
        }
    }
    generatorItr.next();
}

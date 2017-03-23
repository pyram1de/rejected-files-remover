var fs = require('fs');
var JSZip = require('jszip');
var MongoClient = require('mongodb').MongoClient;
var sourceDir = 'z:\\',
    newLine = '\r\n',
    url = 'mongodb://localhost:27017/image-rejects',
    auditFilePath = 'e:\\rejected-calcs\\audit.log',
    auditLine = '',
    targetDirectory = 'e:\\rejected-calcs\\';

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
            if(!fs.existsSync(targetDirectory+calcDir)){
                fs.mkdirSync(targetDirectory+calcDir);
            }
            // find the csv file from each folder and process them.
            var calcDirFiles = fs.readdirSync(calcDirFileDirectory);
            for(var x = 0;x < calcDirFiles.length; x = x + 1){
                if(calcDirFiles[x].substr(-4)==='.csv'){
                    auditLine += '\t' + calcDirFiles[x];
                    var theCSVLocation = calcDirFiles[x];
                    var theCSVFile = fs.readFileSync(sourceDir+calcDir+'\\'+theCSVLocation, 'utf8');
                    var theCSVbyRow = theCSVFile.split(newLine);
                    // make a backup of the CSV file...
                    fs.writeFileSync(targetDirectory+calcDir+'\\'+theCSVLocation+'.bak',theCSVFile);
                    var failedCount = 0,passedCount = 0, size = 0;
                    //identifier,filename,type,date
                    while(theCSVbyRow.length>1){
                        var theRow = theCSVbyRow.shift();
                        var splitRows = theRow.split(',');
                        if(!fs.existsSync(calcDirFileDirectory+'\\'+splitRows[1])){
                            fs.appendFileSync(targetDirectory+calcDir+'\\'+calcDirFiles[x]+'.missing',theRow + newLine); // the csv
                            ++failedCount;
                            continue;
                        }
                        var data = yield rejectedcalcs.findOne(
                                {
                                    "Identifier":Number(splitRows[0], 10),
                                    "file":splitRows[1]
                                },resume);
                        if(data) {
                            // this needs backing up and removing.
                            var fileStat = fs.statSync(calcDirFileDirectory+'\\'+splitRows[1]);
                            size += fileStat.size / 1000 / 1000;
                            // the file in here is a candidate for removal.
                            var zip = new JSZip();
                            var calculation = fs.readFileSync(calcDirFileDirectory+'\\'+data.file);
                            zip.file(data.file,calculation);
                            var compressedData = yield zip.generateAsync({
                                type:'nodebuffer',
                                compression: 'DEFLATE',
                                compressionOptions: {
                                    level: 4
                                }}).then(function(content){
                                    resume(null, content);
                                });
                            fs.writeFileSync(targetDirectory+calcDir+'\\'+data.file.substr(0,data.file.length-4)+'.zip', compressedData);
                            fs.appendFileSync(targetDirectory+calcDir+'\\'+calcDirFiles[x]+'.deleted',theRow + newLine); // the csv
                            /*
                                TODO: remove the file.
                            */
                            // remove the file....
                            
                            fs.unlinkSync(calcDirFileDirectory+'\\'+data.file);
                            
                            ++failedCount;
                        } else {
                            fs.appendFileSync(targetDirectory+calcDir+'\\'+calcDirFiles[x]+'.new',theRow + newLine);
                            ++passedCount;
                        }
                    }
                    
                    if(failedCount>0){
                        /*
                        TODO: at this point
                        we have the option to replace the old csv file with the new csv file
                        */
                        fs.unlinkSync(sourceDir+calcDir+'\\'+theCSVLocation); // remove the old file...
                        var newCSVFile = fs.readFileSync(targetDirectory+calcDir+'\\'+calcDirFiles[x]+'.new','utf8');
                        fs.writeFileSync(sourceDir+calcDir+'\\'+theCSVLocation, newCSVFile); // replace with the new files data.
                    }
                    
                    auditLine = auditLine + '\t' + passedCount+ '\t' + failedCount + '\t' + size;
                    fs.appendFileSync(auditFilePath, auditLine + newLine);
                    break; // stop processing files in this directory. as we found the CSV
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

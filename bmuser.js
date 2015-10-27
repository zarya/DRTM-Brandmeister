var mqtt = require('mqtt');

//Import users 
var Converter = require("csvtojson").Converter;
var converter = new Converter({constructResult:false,delimiter:';'}); //for big csv data 
var user = {}


var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('../DRTM-Backend/repeater.db');
var users = new sqlite3.Database(':memory:');

var wsClient = mqtt.connect('wss://tracker.dstar.su:443/master')
var RTM  = mqtt.connect('mqtt://localhost');

//Create memory db for users
users.serialize(function() {
  users.run("CREATE TABLE users (Call TEXT, ID TEXT UNIQUE, Name TEXT, Location TEXT)");
});

//Load users from ham-digital
converter.on("record_parsed", function (jsonObj) {
  var stmt = users.prepare("INSERT OR REPLACE INTO Users VALUES (?,?,?,?)");
  stmt.run(jsonObj['callsign'],jsonObj['dmrid'],jsonObj['name'],jsonObj['ctry']);
  stmt.finalize(); 
});

require("request").get("http://dmr.ham-digital.net/user_by_call.php").pipe(converter);

console.log("Done loading users");

//Setup MQTT
wsClient.on('connect', function(){
    wsClient.subscribe("Master/+/Session");
})

wsClient.on('error', function(err) {
    console.log(err)
})

wsClient.on('message', function(source, payload) {
  var values = JSON.parse(payload);
  var types = [
      'Application',
      'Repeater',
      'Network'];

  var attributes = new Array();
  var type = values['SessionType'];

  if (type & 0x20)
    attributes.push('Encrypted');
  if (type & 0x02)
    attributes.push('Group');
  if (type & 0x04)
    attributes.push('Voice');
  if (type & 0x08)
    attributes.push('Data');
  if (type & 0x10)
    attributes.push('CSBK');
  attributes.push('Call');


  if (values['ContextID'] != undefined) {
    db.each("SELECT Call,IP FROM repeaters WHERE RID = "+values['ContextID'], function(err, row) {
      repeater = row.Call;

      // Fetch the user 
      var sql = "SELECT Call,Name FROM Users WHERE ID = '"+values['SourceID']+"'";
      users.all(sql, function(err, rows) {
        user = rows[0];
      });

      var content =
        'Server ID:      ' + source + '\n' +
        'Event:          ' + values['Event'] + '\n' +
        'Call Type:      ' + attributes.join(' ') + '\n' +
        'Source ID:      ' + values['SourceID'] + ' ' + user['Call'] + ' ' + user['Name'] +'\n' +
        'Destination ID: ' + values['DestinationID'] + '\n' +
        'Link Type:      ' + types[values['LinkType']] + '\n' +
        'Link Name:      ' + values['LinkName'] + '\n' +
        'Link ID:        ' + values['ContextID'] + '\n' +
        'Repeater:       ' + row.Call + '(' + row.IP + ')\n' +
        'Slot:           ' + values['Slot'] + '\n' +
        'Route:          ' + values['Route'] + '\n';
      console.log(content);

      //Send the data to MQTT
      var now = new Date();
      now = Math.floor(now / 1000);
      RTM.publish("hytera/"+row.IP+"/usrTs"+values['Slot'], user['Call'] + ' ('+user['Name'] + ')')
      RTM.publish("hytera/"+row.IP+"/tlkTs"+values['Slot'], values['DestinationID'].toString())
      RTM.publish("hytera/"+row.IP+"/lastTs"+values['Slot'], now.toString())

    });
  }
});

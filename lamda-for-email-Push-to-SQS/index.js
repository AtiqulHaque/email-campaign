var QUEUE_URL = '****';
var AWS       = require('aws-sdk');
var sqs       = new AWS.SQS({region : 'ap-southeast-1'});
const mysql   = require('mysql');
var moment  = require('moment');


class Database {
  constructor( config ) {
      this.connection = mysql.createConnection( config );
  }
  query( sql, args ) {
      return new Promise( ( resolve, reject ) => {
          this.connection.query( sql, args, ( err, rows ) => {
              if ( err )
                  return reject( err );
              resolve( rows );
          } );
      } );
  }
  close() {
      return new Promise( ( resolve, reject ) => {
          this.connection.end( err => {
              if ( err )
                  return reject( err );
              resolve();
          } );
      } );
  }
}





exports.handler = function(event, context) {
 
  
  	let chekingTime = moment(event.time).add(6, 'hours').utc().format('YYYY-MM-DD H:mm:ss');
  	
  	console.log("Cheking time " + chekingTime);
  	
  	
    let database = new Database({
      host: 'beta-metigey.cpw4x2qhelvv.ap-south-1.rds.amazonaws.com',
      user: 'betametigy',
      password: 'betametigy123',
      port: '3306',
      database: 'metigy-campaign'
    });

    var sql = `SELECT * FROM campaigns where status = 1 and isScheduled = 1 and schedule_time < "${chekingTime}"` ;
    let campaigns =[];
    let campaignsContacts = [];
    let allidentifier = [];


    database.query( sql )
    .then( rows => {
      campaigns = rows;

        if (campaigns.length === 0) {
            throw "No campain found empty schedule campaign";
        }
        
        allidentifier = campaigns.map((data)=>{
          return `"${data.unique_identifier}"`;
        });

        var contactsSQL = `SELECT * FROM contacts 
                where campaign_indentifier IN (${allidentifier.join(",")}) 
                and status = "1"` ;

        console.log(contactsSQL);
        return database.query(contactsSQL);
    } )
    .then(rows => {
          campaignsContacts = rows;

          let updateCampaignSQL = `UPDATE campaigns SET status = 2   where unique_identifier IN (${allidentifier.join(",")})`;

          console.log(updateCampaignSQL);
          return database.query(updateCampaignSQL);
      })
    .then( rows => {
        return database.close();
    }, err => {
        return database.close().then( () => { throw err; } );
    } )
    .then( () => {
        // do something with someRows and otherRows

        let campainMapping = [];

        campaigns.forEach(element => {
          campainMapping[element.unique_identifier] = element;
        });

        campaignsContacts.forEach((contact)=>{



          let body = campainMapping[contact.campaign_indentifier].email_body;
          
          body = body.replace("{{name}}", contact.name);
          
          var params = {
          QueueUrl: QUEUE_URL,
          DelaySeconds: 10,
          MessageAttributes: {
            "Email": {
              DataType: "String",
              StringValue: contact.email
            },
            "Subject": {
              DataType: "String",
              StringValue: campainMapping[contact.campaign_indentifier].email_subject
            }
          },
          MessageBody: body
          
        };

          sqs.sendMessage(params, function(err,data){
          if(err) {
            console.log('error:',"** Failed to write to queue **" + err);
            context.done('error', "** ERROR writing to SQS **");  	// ERROR when trying to enqueue to SQS
          }else{
            console.log('data:',data.MessageId);
            context.done(null,'');  
          }
        });

        });
        context.succeed('Success'); 

        
    } ).catch( err => {
        console.log(err);
        context.succeed('Success'); 
    } );

   
};





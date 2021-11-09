let sendgridLib = require('./sendgrid');

exports.handler = function(event, context) {
    console.log("EVENT: \n" + JSON.stringify(event, null, 2));
   
   console.log(process.env.SEND_GRID_API_KEY);
   
   try {
       
       if(typeof event == 'object' && event["Records"].length > 0 && event["Records"][0].messageAttributes !== 'undefined') {
         
            let toEmail = event["Records"][0].messageAttributes.Email.stringValue;
        
            let toSubject = event["Records"][0].messageAttributes.Subject.stringValue;
            
            let body = event["Records"][0].body;
            
            
             const API_KEY = process.env.SEND_GRID_API_KEY;
    
            sendgridLib.init(API_KEY);
        
            const resp = sendgridLib.sendEmail(toEmail,
                'mailtoatiqul@gmail.com',
                toSubject,
                body,
                body
            );
            
            console.log(resp);
            
            resp.then(data => {
                console.log(data)
                context.succeed('Successfully email sent.');
            }); 
        } else{
             context.succeed('Success'); 
        }
    }
    catch(err) {
        console.log(err);
        context.succeed('Success');
    }

  

}



var express = require('express')
var bodyParser = require('body-parser');
var cassandra = require('cassandra-driver');

var app = express()
app.use(bodyParser.urlencoded({ extended: false }));
var client = new cassandra.Client({contactPoints: ['127.0.0.1'], keyspace: 'demo'});

app.get('/', function (req, res) {
  res.sendStatus('Hello World!')
})

// accept POST request on the homepage
//app.post('/user', function (req, res) {
//  res.sendStatus('Got a POST request');
//})

// accept POST request on the homepage
app.post('/user', function (req, res) {
  var lastname=req.body.lastname;
  var age=req.body.age;
  var city=req.body.city;
  var email=req.body.email;
  var firstname=req.body.firstname;

  console.log("lastname = "+lastname+", age= "+age+", city= "+city+", email= "+email+", firstname= "+firstname);
  client.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ( '" + lastname + "',"
  + age + ",'" + city + "' ,'" + email + "','" + firstname + "')",
   function (err, result) {
      if (!err) {
        res.sendStatus("Inserted");
      } else {
        res.sendStatus(404)
      }
  })
})




// Accept a GET and get a user record
// Ex: http://127.0.0.1:3000/user/Jones will get lastname Jones from the DB
app.get('/user/:lastname',function (req, res) {

  client.execute("SELECT lastname, age, city, email, firstname FROM users WHERE lastname='" + req.params.lastname + "'", function (err, result) {
    if (!err){
      if ( result.rows.length > 0 ) {
        var user = result.rows[0];
        console.log("name = %s, age = %d", user.firstname, user.age);
        res.sendStatus(user)
      } else {
        res.sendStatus(404);
      }
    }
  });
})

// Accept a DELETE and get a user record
// Ex: http://127.0.0.1:3000/user/Jones will delete lastname Jones from the DB
app.delete('/user/:lastname', function (req, res) {
  client.execute("DELETE FROM users WHERE lastname = '" + req.params.lastname + "'", function (err, result) {
    var user = result.Type;
    console.log(user);

      if (!err) {
        res.sendStatus("Deleted");
      } else {
        res.sendStatus(404)
      }
  });
})

app.put('/user/:lastname', function (req, res) {
  var age=req.body.age;

  console.log("lastname = "+req.params.lastname+", age= "+age);
    client.execute("UPDATE users SET age = "+ age +" WHERE lastname = '" + req.params.lastname + "'",
    function (err, result) {
        if (!err) {
          res.sendStatus("Updated");
        } else {
          res.sendStatus(404)
        }
    });
})

var server = app.listen(3000, function () {

  var host = server.address().address
  var port = server.address().port

  console.log('Example app listening at http://%s:%s', host, port)

})

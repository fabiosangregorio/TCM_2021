const ObjectID = require("mongodb").ObjectID
const connect_to_db = require('./db');
// GET BY TALK HANDLER
const talk = require('./Talk');
module.exports.get_watchnext_by_idx = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  console.log('Received event:', JSON.stringify(event, null, 2));
  let body = {}
  if (event.body) {
    body = JSON.parse(event.body)
  }
  // set default
  if (!body.idx) {
    callback(null, {
      statusCode: 500,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Could not fetch the talks. Idx is null.'
    })
  }
  if (!body.doc_per_page) {
    body.doc_per_page = 10
  }
  if (!body.page) {
    body.page = 1
  }
  connect_to_db().then(() => {
    console.log('=> get_all talks');
    talk.find({ "_id": body.idx })
      .skip((body.doc_per_page * body.page) - body.doc_per_page)
      .limit(body.doc_per_page)
      .then(talks => {
          const watch_next_ids = talks[0].watch_next;
          console.log(watch_next_ids)
          talk.find({_id: { $in: watch_next_ids }})
            .then(watchnext => { 
                callback(null, {
                  statusCode: 200,
                  body: JSON.stringify(watchnext)
                })
            })
            .catch(err =>
              callback(null, {
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: "dentro" + err
              })
            );
      })
      .catch(err =>
        callback(null, {
          statusCode: err.statusCode || 500,
          headers: { 'Content-Type': 'text/plain' },
          body: err
        })
      );
  });
};
const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    title: String,
    url: String,
    details: String,
    watch_next: Array,
    main_speaker: String
}, { collection: 'tedx_data' });

module.exports = mongoose.model('talk', talk_schema);
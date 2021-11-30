const knex = require('knex')({
    client: 'mysql2',
    connection: {
      host: 'mysql-do-user-7045040-0.b.db.ondigitalocean.com',
      user: 'bms',
      port:25060,
      password: 'e_NQ2k5mnb7Uly_v',
      database: 'winfit'
    },
     pool:{
      min: 2,
      max: 20,
    }
  });
  
  module.exports = knex;
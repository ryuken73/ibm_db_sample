const ibmdb = require('./lib/ibm_db');
const dbConfig = require('./dbConfig.json');

const MUSICDB_CONNECTION_STRING = dbConfig.DB2['MUSICDBDEV'];
const musicdb = ibmdb.connectDB(MUSICDB_CONNECTION_STRING, global.logger);

async function main(){
  try {

    // simple select
    const result = await musicdb.query('select * from syscat.tables')

    // select with where
    const params = ['SYSCAT', 'SYSIBM']
    const result1 = await musicdb.query('select * from syscat.tables where tabschema = ? and owner = ?', params);

    // update data
    const updateParams = ['k', 'a']
    const result2 = await musicdb.query('update mbsinst.test set a = ? where a = ?', updateParams);
    // update data returns affected count(execute)
    const result3 = await musicdb.execute('update mbsinst.test set a = ? where a = ?', updateParams);

    // insert data
    const insertParams = ['a']
    const result4 = await musicdb.execute('insert into mbsinst.test values (?)', insertParams);

    console.log(result4)
  } catch (err) {
    console.error(err)
  }
}

main();
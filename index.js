const express = require("express")
const {Client} = require('pg')
const Cursor = require('pg-cursor')
const Filter = require('bad-words');
filter = new Filter();
const parquet = require('parquetjs')
const fs = require('fs')

const dotenv = require('dotenv');

if (process.env.NODE_ENV !== 'production')
    dotenv.config();

const path = require('path')

const {execute} = require('@getvim/execute');


const client = new Client({
    user: process.env.DB_USER_NAME,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE_NAME,
    password: process.env.DB_USER_PASSWORD,
    port: process.env.DB_PORT
})

client.connect(err => {
    if (err) throw err;
    console.log("Connected!")
})

client.on('error', (err) => {
    console.error('something bad has happened!', err.stack)
})

const app = express()
const port = process.env.PORT || 3000


app.use('/archive', express.static('archive'))
app.use('/backup', express.static('backup'))

app.get("/health", (req, res) => res.json({status: "UP"}))

app.get("/clean-by-age", async (req, res) => {

    // Filter and delete all comments that were made on or before 9th October, 2023
    const result = await client.query("DELETE FROM COMMENTS WHERE timestamp < '09-10-2023 00:00:00'")

    if (result.rowCount > 0) {
        res.json({message: "Cleaned up " + result.rowCount + " rows successfully!"})
    } else {
        res.json({message: "Nothing to clean up!"})
    }
})

app.get("/emoji", async (req, res) => {

    // Define a list of emojis that need to be converted
    const emojiMap = {
        xD: '😁',
        ':)': '😊',
        ':-)': '😄',
        ':jack_o_lantern:': '🎃',
        ':ghost:': '👻',
        ':santa:': '🎅',
        ':christmas_tree:': '🎄',
        ':gift:': '🎁',
        ':bell:': '🔔',
        ':no_bell:': '🔕',
        ':tanabata_tree:': '🎋',
        ':tada:': '🎉',
        ':confetti_ball:': '🎊',
        ':balloon:': '🎈'
    }

    // Build the SQL query adding conditional checks for all emojis from the map
    let queryString = "SELECT * FROM COMMENTS WHERE"

    queryString += " COMMENT_TEXT LIKE '%" + Object.keys(emojiMap)[0] + "%' "

    if (Object.keys(emojiMap).length > 1) {
        for (let i = 1; i < Object.keys(emojiMap).length; i++) {
            queryString += " OR COMMENT_TEXT LIKE '%" + Object.keys(emojiMap)[i] + "%' "
        }
    }

    queryString += ";"

    const result = await client.query(queryString)

    if (result.rowCount === 0) {
        res.json({message: "No rows to clean up!"})
    } else {
        for (let i = 0; i < result.rows.length; i++) {

            const currentRow = result.rows[i]
            let emoji

            // Identify each row that contains an emoji along with which emoji it contains
            for (let j = 0; j < Object.keys(emojiMap).length; j++) {
                if (currentRow.comment_text.includes(Object.keys(emojiMap)[j])) {
                    emoji = Object.keys(emojiMap)[j]
                    break
                }
            }

            // Replace the emoji in the text and update the row before moving on to the next row
            const updateQuery = "UPDATE COMMENTS SET COMMENT_TEXT = '" + currentRow.comment_text.replace(emoji, emojiMap[emoji]) + "' WHERE COMMENT_ID = " + currentRow.comment_id + ";"

            await client.query(updateQuery)
        }

        res.json({message: "All emojis cleaned up successfully!"})
    }


})

app.get('/conditional', async (req, res) => {

    // Filter and delete all comments that are not linked to any active posts
    const result = await client.query("DELETE FROM COMMENTS WHERE post_id NOT IN (SELECT post_id from Posts);")

    if (result.rowCount > 0) {
        res.json({message: "Cleaned up " + result.rowCount + " rows successfully!"})
    } else {
        res.json({message: "Nothing to clean up!"})
    }
})

app.get('/obscene', async (req, res) => {

    // Query all comments using a cursor, reading only 10 at a time
    const queryString = "SELECT * FROM COMMENTS;"

    const cursor = client.query(new Cursor(queryString))

    let rows = await cursor.read(10)

    const affectedRows = []

    while (rows.length > 0) {

        for (let i = 0; i < rows.length; i++) {
            // Check each comment for profane content
            if (filter.isProfane(rows[i].comment_text)) {
                affectedRows.push(rows[i])
            }
        }

        rows = await cursor.read(10)
    }

    cursor.close()

    // Update each comment that has profane content with a censored version of the text
    for (let i = 0; i < affectedRows.length; i++) {
        const row = affectedRows[i]
        const updateQuery = "UPDATE COMMENTS SET COMMENT_TEXT = '" + filter.clean(row.comment_text) + "' WHERE COMMENT_ID = " + row.comment_id + ";"
        await client.query(updateQuery)
    }

    res.json({message: "Cleanup complete"})

})


app.get("/reindex", async (req, res) => {

    // Run the REINDEX command as needed
    await client.query("REINDEX TABLE Users;")

    res.json({message: "Reindexed table successfully"})
})

app.get('/archive', async (req, res) => {

    // Query all comment through a cursor, reading only 10 at a time
    const queryString = "SELECT * FROM COMMENTS;"

    const cursor = client.query(new Cursor(queryString))

    // Define the schema for the parquet file
    let schema = new parquet.ParquetSchema({
        comment_id: { type: 'INT64' },
        post_id: { type: 'INT64' },
        user_id: { type: 'INT64' },
        comment_text: { type: 'UTF8' },
        timestamp: { type: 'TIMESTAMP_MILLIS' }
    });

    // Open a parquet file writer
    let writer = await parquet.ParquetWriter.openFile(schema, 'archive/archive.parquet');

    let rows = await cursor.read(10)

    while (rows.length > 0) {

        for (let i = 0; i < rows.length; i++) {
            // Write each row from table to the parquet file
            await writer.appendRow(rows[i])
        }

        rows = await cursor.read(10)
    }

    await writer.close()

    // Redirect user to the file path to allow them to download the file
    res.redirect("/archive/archive.parquet")
})

app.get('/backup', async (req, res) => {

    // Create a name for the backup file
    const fileName = "database-backup-" + new Date().valueOf() + ".tar";

    // Execute the pg_dump command to generate the backup file
    execute("pg_dump -U " + process.env.DB_USER_NAME + " -d " + process.env.DB_DATABASE_NAME + " -f backup/" + fileName + " -F t",).then(async () => {
        console.log("Backup created");
        res.redirect("/backup/" + fileName)
    }).catch(err => {
        console.log(err);
        res.json({message: "Something went wrong"})
    })


})

app.get('/restore', async (req, res) => {

    const dir = 'backup'

    // Sort the backup files according to when they were created
    const files = fs.readdirSync(dir)
        .filter((file) => fs.lstatSync(path.join(dir, file)).isFile())
        .map((file) => ({ file, mtime: fs.lstatSync(path.join(dir, file)).mtime }))
        .sort((a, b) => b.mtime.getTime() - a.mtime.getTime());

    if (!files.length){
        res.json({message: "No backups available to restore from"})
    }

    const fileName = files[0].file


    // Restore the database from the chosen backup file
    execute("pg_restore -cC -d " + process.env.DB_USER_NAME + " " + "backup/" + fileName)
        .then(async ()=> {
            console.log("Restored");
        }).catch(err=> {
        console.log(err);
    })

    res.json({message: "Backup restored"})
})
app.listen(port, () => {
    console.log("Server running at port: " + port);
});
const express = require("express")

const app = express()
const port = 3000

app.get("/health", (req, res) => res.json({status: "UP"}))

app.listen(port, () => {
    console.log(`Server running at port: ${port}`);
});
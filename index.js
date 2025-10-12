const express = require('express');
const cookieParser = require('cookie-parser');
const path = require('path');
require('dotenv').config();

const app = express();

app.use((req, res, next) => {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] ${req.method} ${req.originalUrl}`);
    next();
});
const csRouter = require('./routes/cs');

app.use(cookieParser());
app.use('/cs', csRouter);
app.use('/cs/static', express.static(path.join(__dirname, 'views')));

app.listen(3003, () => {
    console.log('✅ 서버 실행 중: http://localhost:3003');
});

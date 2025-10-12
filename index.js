const express = require('express');
const cookieParser = require('cookie-parser');
const path = require('path');
require('dotenv').config();

const app = express();
const csRouter = require('./routes/cs');

app.use(cookieParser());
app.use('/cs', csRouter);
app.use('/cs/static', express.static(path.join(__dirname, 'views')));

app.listen(3003, () => {
    console.log('✅ 서버 실행 중: http://localhost:3003');
});

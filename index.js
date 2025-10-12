const express = require('express');
const cookieParser = require('cookie-parser');
require('dotenv').config();

const app = express();
const csRouter = require('./routes/cs');

// 정적 파일 서빙: /cs 경로에서 제공
app.use('/cs', express.static(__dirname));
app.use(cookieParser());

app.use('/cs', csRouter);

app.listen(3003, () => {
    console.log('✅ 서버 실행 중: http://localhost:3003');
});

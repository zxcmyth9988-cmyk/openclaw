import express from 'express';

const app = express();
const port = process.env.PORT || 18789;

app.get('/', (req, res) => {
  res.send('Hello from makemoneywithai!');
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
});

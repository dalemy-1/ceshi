const express = require('express');
const axios = require('axios');
const app = express();
const port = 3000;  // 选择一个端口

app.get('/api/products', async (req, res) => {
  try {
    const response = await axios.get('http://154.48.226.95:5001/admin/Product/export_csv');
    res.send(response.data);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error fetching product data');
  }
});

app.listen(port, () => {
  console.log(`Proxy server running at http://localhost:${port}`);
});

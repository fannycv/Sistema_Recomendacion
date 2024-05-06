const express = require('express');
const { Pool } = require('pg');
const app = express();

const pool = new Pool({
    host: 'mi_postgres_container',
    port: 5432,
    database: 'recomendaciones_libros',
    user: 'postgres',
    password: 'password'
});

app.set('view engine', 'ejs');

app.get('/books', async (req, res) => {
    try {
        const result = await pool.query("SELECT * FROM books");
        const books = result.rows;
        res.render('index', { books });
    } catch(error) {
        console.error(error);
        res.status(500).send('Error obteniendo los libros');
    }
});

app.listen(3000, () => {
    console.log('Servidor Node.js escuchando en el puerto 3000');
});

const express = require('express');
const bodyParser = require('body-parser');
const mysql = require('mysql2');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
app.use(bodyParser.json());

//Configuración de conexión a MySQL
const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'middleware'
});

db.connect(err => {
    if (err) {
        console.error('Error conectando a la base de datos:', err.stack);
        return;
    }
    console.log('Conectado a la base de datos MySQL');
});

// Función para registrar eventos en la tabla log
function registrarLog(descripcion) {
    const query = 'INSERT INTO log (descripcion) VALUES (?)';
    db.query(query, [descripcion], (error, results) => {
        if (error) {
            console.error('Error al insertar log:', error);
        } else {
            console.log('Log registrado:', descripcion);
        }
    });
}

// Función para verificar y sincronizar registros desde la API PHP
async function sincronizarUsuarios() {
    try {
        // Consumo de la API PHP para obtener todos los registros de usuario
        const response = await axios.get('http://localhost/api_test/guardar_usuario.php'); // URL de la API PHP

        if (response.data) {
            for (const usuario of response.data) {
                const { nombre, apellido_paterno, apellido_materno, fono } = usuario;

                // Transformación de datos (ejemplo: convertir el nombre del usuario a mayúsculas)
                const nombreTransformado = nombre.toUpperCase();

                // Verificar si el registro ya existe en usuarios_test
                const checkQuery = 'SELECT COUNT(*) AS count FROM usuarios_test WHERE nombre = ? AND apellido_paterno = ? AND apellido_materno = ? AND fono = ?';
                const [rows] = await db.promise().query(checkQuery, [nombreTransformado, apellido_paterno, apellido_materno, fono]);

                if (rows[0].count === 0) {
                    // Insertar datos en la tabla usuarios_test si no existe
                    const insertQuery = 'INSERT INTO usuarios_test (nombre, apellido_paterno, apellido_materno, fono) VALUES (?, ?, ?, ?)';
                    db.query(insertQuery, [nombreTransformado, apellido_paterno, apellido_materno, fono], (error, results) => {
                        if (error) {
                            const errorMessage = `Error al insertar datos en usuarios_test: ${error.message}`;
                            console.error(errorMessage);
                            registrarLog(errorMessage);
                        } else {
                            console.log('Datos insertados en usuarios_test:', results);
                            registrarLog(`Datos insertados en usuarios_test para usuario: ${nombreTransformado}`);
                        }
                    });
                } else {
                    registrarLog('Consumo de API por GET ERROR');
                    // Registrar en log que el usuario ya existe en usuarios_test
                    const logMessage = `Usuario ${nombreTransformado} ya existente, no insertado.`;
                    console.log(logMessage);
                    registrarLog(logMessage);
                }
            }
        }
    } catch (error) {
        const errorMessage = `Error al consumir la API de PHP: ${error.message}`;
        console.error(errorMessage);
        registrarLog(errorMessage);
    }
}

// Cuando se guarda la data desde el formulario en la tabla usuario
// Ruta para recibir el webhook y guardar directamente en usuarios_test
app.post('/webhook', (req, res) => {
    const { nombre, apellido_paterno, apellido_materno, fono } = req.body;

    // Transformación de datos (ej: convertir nombre del usuario a mayúsculas)
    const nombreTransformado = nombre.toUpperCase();
    
    const query = 'INSERT INTO usuarios_test (nombre, apellido_paterno, apellido_materno, fono) VALUES (?, ?, ?, ?)';
    db.query(query, [nombreTransformado, apellido_paterno, apellido_materno, fono], (error, results) => {
        if (error) {
            //console.error('Error al insertar datos en usuarios_test:', error);
            registrarLog(`Error al insertar datos en usuarios_test desde webhook: ${error.message}`);
            return res.status(500).send('Error al insertar datos');
        }
        //console.log('Datos insertados en usuarios_test:', results);
        registrarLog(`El usuario ${nombreTransformado} fue insertado en usuarios_test desde webhook`);
        res.status(200).send('Datos recibidos y guardados');
    });
});

// Ruta para recibir múltiples registros desde el webhook y procesarlos
app.post('/webhook_batch', async (req, res) => {
    const usuarios = req.body;
    registrarLog('Inicio de webhook_batch');

    for (const usuario of usuarios) {
        const { id_usuario, nombre, apellido_paterno, apellido_materno, fono } = usuario;

        // Transformación de datos
        const nombreTransformado = nombre.toUpperCase();

        // Verificación de duplicados y registro en usuarios_test o log
        const checkQuery = 'SELECT COUNT(*) AS count FROM usuarios_test WHERE id_usuario = ?';
        const [rows] = await db.promise().query(checkQuery, [id_usuario]);

        if (rows[0].count === 0) {
            const insertQuery = 'INSERT INTO usuarios_test (id_usuario, nombre, apellido_paterno, apellido_materno, fono) VALUES (?, ?, ?, ?, ?)';
            db.query(insertQuery, [id_usuario, nombreTransformado, apellido_paterno, apellido_materno, fono], (error, results) => {
                if (error) {
                    registrarLog(`Error al insertar en usuarios_test: ${error.message}`);
                } else {
                    registrarLog(`Usuario ${nombreTransformado} con ID ${id_usuario} insertado en usuarios_test`);
                }
            });
        } else {
            registrarLog(`Usuario ${nombreTransformado} ya existe en usuarios_test con ID ${id_usuario}, no insertado`);
        }
    }
    res.status(200).send('Procesamiento de usuarios completado');
});

// Ruta para sincronizar registros desde la API hecha con PHP
app.get('/sincronizar', async (req, res) => {
    registrarLog('sincroniza datos');
    await sincronizarUsuarios();
    res.status(200).send('Sincronización completada');
});

// Configuración del cron job para ejecutar la sincronización cada 10 minutos
cron.schedule('*/10 * * * *', () => {
    // Para detener el cron job
    //cronJob.stop();  
    registrarLog('Inicio de cron');
    console.log('Ejecutando cron job para sincronizar usuarios');
    sincronizarUsuarios();
});

app.listen(3000, () => {
    console.log('Servidor de Node.js escuchando en el puerto 3000');
});

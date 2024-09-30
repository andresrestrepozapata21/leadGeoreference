// Importar módulos necesarios
import mysql from 'mysql';
import cliProgress from 'cli-progress';
import logger from './utils/logger.js';
import dotenv from 'dotenv';
// Configuración de MySQL a BBDD testing
dotenv.config();
// Configuración de MySQL a BBDD testing
const connection = mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD,
    database: process.env.DATABASE
});

// Promisificar las consultas
function queryAsync(sql, values) {
    return new Promise((resolve, reject) => {
        connection.query(sql, values, (error, results) => {
            if (error) {
                return reject(error);
            }
            resolve(results);
        });
    });
}

// Función para obtener todos los leads de la base de datos
async function getLeadsData() {
    const queryLead = `
    SELECT BIN_TO_UUID(id) AS id,
           campaign_name,
           post_code,
           id_country 
    FROM \`lead\`
    WHERE
    id_country IS NULL
  `;
    return await queryAsync(queryLead);
}

// Función principal para ejecutar el flujo completo
async function main() {
    try {
        connection.connect();

        const datos = await getLeadsData();
        logger.info(`Número de leads a procesar: ${datos.length}`);
        // Inicializar la barra de progreso
        const progressBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
        progressBar.start(datos.length, 0);
        for (const lead of datos) {
            let idLead = lead.id;
            let campaign_name = lead.campaign_name;
            if(campaign_name == null){
                campaign_name = '';
            }
            if (campaign_name.includes('ES') || campaign_name.includes('Spain') || campaign_name.includes('España')) {
                const queryUpdate = `UPDATE \`lead\` SET id_country = 'ES' WHERE \`lead\`.id = UUID_TO_BIN(?)`;
                await queryAsync(queryUpdate, [idLead]);
            } else if (campaign_name.includes('FR') || campaign_name.includes('France')) {
                const queryUpdate = `UPDATE \`lead\` SET id_country = 'FR' WHERE \`lead\`.id = UUID_TO_BIN(?)`;
                await queryAsync(queryUpdate, [idLead]);
            } else if (campaign_name.includes('IT') || campaign_name.includes('Italy') || campaign_name.includes('Italia')) {
                const queryUpdate = `UPDATE \`lead\` SET id_country = 'IT' WHERE \`lead\`.id = UUID_TO_BIN(?)`;
                await queryAsync(queryUpdate, [idLead]);
            } else if (campaign_name.includes('PT') || campaign_name.includes('Portugal')) {
                const queryUpdate = `UPDATE \`lead\` SET id_country = 'PT' WHERE \`lead\`.id = UUID_TO_BIN(?)`;
                await queryAsync(queryUpdate, [idLead]);
            } 
            // Actualizar la barra de progreso
            progressBar.increment();
        }
        // Finalizar la barra de progreso
        progressBar.stop();

    } catch (error) {
        console.error('Error:', error);
    } finally {
        connection.end();
    }
}

// Ejecutar la función principal
main();
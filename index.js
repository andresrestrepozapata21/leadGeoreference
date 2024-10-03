// Importar módulos necesarios
import mysql from 'mysql2';
import cliProgress from 'cli-progress';
import logger from './utils/logger.js';
import dotenv from 'dotenv';
// Configuración de MySQL a BBDD testing
dotenv.config();
// Configuración de MySQL a BBDD testing
const connection = mysql.createConnection({
    host: 'flipo-sql-instance-1.cmgjtomlonmm.eu-west-1.rds.amazonaws.com',
    user: 'admin',
    password: '4#Am5C)9y~C*;yfFxA',
    database: 'test'
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
           post_code,
           id_country 
    FROM \`lead\`
    WHERE
    preassigned_optic_process = 0 AND
    id_country IS NOT NULL
  `;
    return await queryAsync(queryLead);
}

// Función principal para insertar en la tabla intermedia entre ópticas y leads
async function mainInsertOpticLead(datos) {
    // Inicializar la barra de progreso
    const progressBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
    progressBar.start(datos.length, 0);

    for (const lead of datos) {
        const idLead = lead.id;
        const postCode = lead.post_code;
        const idCountry = lead.id_country;

        const queryGeographicCache = `
            SELECT * 
            FROM geolocation_cache
            WHERE searched_term = ? AND id_country = ?
        `;
        const results = await queryAsync(queryGeographicCache, [postCode, idCountry]);
        if (results.length > 0) {
            logger.info(`Lead ID: ${idLead}, Country: ${idCountry}, Post Code: ${postCode}, Latitude: ${results[0].lat}, Longitude: ${results[0].lng}`);
            const { lat, lng, id_country } = results[0];
            const resultNearOptics = await getThreeNearOptics(lat, lng, 60);

            if (resultNearOptics.length > 0) {
                let arrayOrdered = [];
                resultNearOptics.sort((a, b) => {
                    const a_distance = distance(a.latitude, a.longitude, lat, lng);
                    a.distanceFromCords = a_distance;
                    const b_distance = distance(b.latitude, b.longitude, lat, lng);
                    b.distanceFromCords = b_distance;
                    return a_distance - b_distance;
                });
                if (resultNearOptics[0]) {
                    arrayOrdered.push(resultNearOptics[0]);
                }
                if (resultNearOptics[1]) {
                    arrayOrdered.push(resultNearOptics[1]);
                }
                if (resultNearOptics[2]) {
                    arrayOrdered.push(resultNearOptics[2]);
                }
                let ranking = 1;
                for (const optic of arrayOrdered) {
                    const { id } = optic;
                    const queryInsertOpticsLead = `
                        INSERT INTO optic_lead(id_lead_ol, id_optic_ol, ranking) VALUES (UUID_TO_BIN('${idLead}'), UUID_TO_BIN('${id}'), ${ranking})
                    `;
                    await queryAsync(queryInsertOpticsLead);
                    logger.info(`Lead ID: ${idLead}, Optic ID: ${id}, Ranking: ${ranking}`);
                    ranking++;
                }
            }
        } else {
            //console.log(`No geolocation data found for post code: ${postCode}`);
        }

        const queryUpdateLead = `
            UPDATE \`lead\` SET preassigned_optic_process= 1 WHERE \`lead\`.id = UUID_TO_BIN('${idLead}')
        `;
        await queryAsync(queryUpdateLead);

        // Actualizar la barra de progreso
        progressBar.increment();
    }

    // Finalizar la barra de progreso
    progressBar.stop();
}

// Función para obtener las 3 ópticas cercanas a la ubicación del lead
async function getThreeNearOptics(lat, lng, radius) {
    const queryOptics = `
        SELECT BIN_TO_UUID(optic.id) AS id, id_code, latitude, longitude
        FROM optic
        LEFT JOIN country ON optic.id_country = country.id
        WHERE
        6371 * ACOS(
            COS(RADIANS(${lat})) * COS(RADIANS(optic.latitude)) * COS(RADIANS(optic.longitude) - RADIANS(${lng})) +
            SIN(RADIANS(${lat})) * SIN(RADIANS(optic.latitude))
        ) < ${radius}
        AND optic.is_active = 1
        LIMIT 3
    `;
    return await queryAsync(queryOptics);
}
// Función para calcular la distancia entre dos puntos en km
function distance(lat1, lon1, lat2, lon2) {
    const p = 0.017453292519943295;
    const c = Math.cos;
    const a =
        0.5 -
        c((lat2 - lat1) * p) / 2 +
        (c(lat1 * p) * c(lat2 * p) * (1 - c((lon2 - lon1) * p))) / 2;
    return 12742 * Math.asin(Math.sqrt(a));
}

// Función principal para ejecutar el flujo completo
async function main() {
    try {
        connection.connect();

        const datos = await getLeadsData();
        logger.info(`Número de leads a procesar: ${datos.length}`);
        await mainInsertOpticLead(datos);

    } catch (error) {
        console.error('Error:', error);
    } finally {
        connection.end();
    }
}

// Ejecutar la función principal
main();
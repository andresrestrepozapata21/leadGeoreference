// Importar módulos necesarios
import mysql from 'mysql';
import cliProgress from 'cli-progress';

// Configuración de MySQL a BBDD testing
const connection = mysql.createConnection({
    host: '',
    user: '',
    password: '',
    database: ''
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
           post_code 
    FROM \`lead\`
    WHERE
    preassigned_optic_process = 0
    LIMIT 500
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

        const queryGeographicCache = `
            SELECT * 
            FROM geolocation_cache
            WHERE searched_term = ?
        `;
        const results = await queryAsync(queryGeographicCache, [postCode]);

        if (results.length > 0) {
            const { lat, lng, id_country } = results[0];
            console.log(`Lead ID: ${idLead}, Country: ${id_country}, Post Code: ${postCode}, Latitude: ${lat}, Longitude: ${lng}`);
            const resultNearOptics = await getThreeNearOptics(lat, lng, 60);

            if (resultNearOptics.length > 0) {
                resultNearOptics.sort((a, b) => {
                    const a_distance = distance(a.latitude, a.longitude, lat, lng);
                    a.distanceFromCords = a_distance;
                    const b_distance = distance(b.latitude, b.longitude, lat, lng);
                    b.distanceFromCords = b_distance;
                    return a_distance - b_distance;
                });

                let ranking = 1;
                for (const optic of resultNearOptics) {
                    const { id } = optic;
                    const queryInsertOpticsLead = `
                        INSERT INTO optic_lead(id_lead_ol, id_optic_ol, ranking) VALUES (UUID_TO_BIN('${idLead}'), UUID_TO_BIN('${id}'), ${ranking})
                    `;
                    console.log(queryInsertOpticsLead);
                    await queryAsync(queryInsertOpticsLead);
                    ranking++;
                }
            }
        } else {
            console.log(`No geolocation data found for post code: ${postCode}`);
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
        SELECT BIN_TO_UUID(optic.id) AS id
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
        await mainInsertOpticLead(datos);

    } catch (error) {
        console.error('Error:', error);
    } finally {
        connection.end();
    }
}

// Ejecutar la función principal
main();

// Importo los modulos que necesito
import mysql from 'mysql';
// Configuración de MySQL a BBDD testing
const connection = mysql.createConnection({
    host: 'test-for-test2.cmgjtomlonmm.eu-west-1.rds.amazonaws.com',
    user: 'admin',
    password: '4#Am5C)9y~C*;yfFxA',
    database: 'test'
});

// Promisificar las consultas
function queryAsync(sql, values) {
    // Creo una promesa que me realizar la conexion a la base de datos cada que lo necesite
    return new Promise((resolve, reject) => {
        // conecto y envio la sql que me llegue por parametro
        connection.query(sql, values, (error, results) => {
            if (error) {
                return reject(error);
            }
            resolve(results);
        });
    });
}

// Función para obtener todos los lead de la base de datos
async function getLeadsData() {
    // SQL para obtener todos los leads
    const queryLead = `
    SELECT BIN_TO_UUID(id) AS id,
           post_code 
    FROM \`lead\`
    WHERE
    preassigned_optic_process = 0
    LIMIT 500
  `;
    // Llamo mi metodo para consumar la SQL
    return await queryAsync(queryLead);
}

// Función principal para insertar en la tabla intermedia entre opticas y leads
async function mainInsertOpticLead(datos) {
    // Recorro los lead que me llegan por parametro
    for (const lead of datos) {
        // Capturo las variables que necesito
        const idLead = lead.id;
        const postCode = lead.post_code;
        // SQl para obtener las coordenada de ese lead
        const queryGeographicCache = `
            SELECT * 
            FROM geolocation_cache
            WHERE searched_term = ?
        `;
        // Llamo y guardo en una variable el resultado de la consulta SQL.
        const results = await queryAsync(queryGeographicCache, [postCode]);
        // Si este lead tiene resultados
        if (results.length > 0) {
            // Capturo las coordenadas y el pais
            const { lat, lng, id_country } = results[0];
            // Imprimo en consola para control
            console.log(`Lead ID: ${idLead}, Country: ${id_country}, Post Code: ${postCode}, Latitude: ${lat}, Longitude: ${lng}`);
            // Segun las coordenadas que tengo, llamo el metodo que me trae las 3 opticas mas cercanas en un radio de 60 km
            const resultNearOptics = await getThreeNearOptics(lat, lng, 60);
            // Si efectivamente tengo opticas cercanas
            if (resultNearOptics.length > 0) {
                // Declaro variable para settear el ranking
                let ranking = 1;
                // Recorro cuales son las opticas para extraerles el ID
                for (const optic of resultNearOptics) {
                    // Capturo el id de la optica
                    const { id } = optic;
                    // SQL para insertar en la tabla intermedia el registro con los ID correspondientes
                    const queryInsertOpticsLead = `
                        INSERT INTO optic_lead(id_lead_ol, id_optic_ol, ranking) VALUES (UUID_TO_BIN('${idLead}'), UUID_TO_BIN('${id}'), ${ranking})
                    `;
                    // Console log para control
                    console.log(queryInsertOpticsLead);
                    // Llamo el metodo para consultar la SQL
                    await queryAsync(queryInsertOpticsLead);
                    // Aumento el ranking segun el orden
                    ranking++;
                }
            }
        } else {
            // Cuando no se encuetran georeferencias
            console.log(`No geolocation data found for post code: ${postCode}`);
        }
        // Actualizo el lead
        const queryUpdateLead = `
            UPDATE \`lead\` SET preassigned_optic_process= 1 WHERE \`lead\`.id = UUID_TO_BIN('${idLead}')
        `;
        // Llamo el metodo para consultar la SQL
        await queryAsync(queryUpdateLead);
    }
}

// Función para obtener las 3 opticas cercanas a la ubicacion del lead
async function getThreeNearOptics(lat, lng, radius) {
    // SQL para obtener las opticas cercanas en un radio de 60km y maximo 3 opticas
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
    // Llamo el metodo que me permite consultar la sql
    return await queryAsync(queryOptics);
}

// Función principal para ejecutar el flujo completo
async function main() {
    try {
        // Conectar a la base de datos
        connection.connect();

        // Llamo los metod que necesito
        const datos = await getLeadsData();
        await mainInsertOpticLead(datos);

    } catch (error) {
        console.error('Error:', error);
    } finally {
        // Cerrar la conexión a la base de datos
        connection.end();
    }
}

// Ejecutar la función principal
main();
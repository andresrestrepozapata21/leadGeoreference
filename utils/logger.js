// I import libraries from winston
import { createLogger, format, transports } from 'winston';

// I defined const logger with config log
const logger = createLogger({
    format: format.combine(
        format.simple(),
        format.timestamp(),
        //format.printf(info => `[${info.timestamp}] ${info.level} ${info.message}`)
    ),
    
    transports: [
        new transports.File({
            maxSize: 5120000,
            maxFiles: 5,
            filename: `logs/log_api.txt`
        })
    ]
});

// Case of the error write logger
logger.on('error', (error) => {
    console.error('Error en el logger:', error);
});

// Export logger for to use anytime
export default logger;

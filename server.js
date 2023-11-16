const express = require('express');
const { Storage } = require('@google-cloud/storage');
const app = express();
const cors = require('cors');
const fs = require('fs');
const csv = require('csv-parser');
const yaml = require('js-yaml');
const xml2js = require('xml2js');
const path = require('path');
const util = require('util');
const port = 3000; 
let airportsData = [];
let aircraftsData = [];
let passengersData = [];
let flightsData = [];
app.use(cors());


app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});


const storage = new Storage({
  keyFilename: './taller-integracion-310700-657496f7c70e.json', 
});

const bucketName = '2023-2-tarea3'; 

const bucket = storage.bucket(bucketName);

bucket.getFiles()
  .then((results) => {
    const files = results[0];
    console.log('Files in the bucket:');
    files.forEach((file) => {
      console.log(file.name);
      if (file.name.endsWith('.xml')) {
        parseXMLFromGCS(file);
      } else if (file.name.endsWith('.csv')) {
        parseCSVFromGCS(file);
      } else if (file.name.endsWith('.json')) {
        parseJSONFromGCS(file);
      } else if (file.name.endsWith('.yaml') || file.name.endsWith('.yml')) {
        parseYAMLFromGCS(file);
      }
    });
  })
  .catch((error) => {
    console.error('Error listing files in the bucket:', error);
  });


  function ensureDirectoryExistence(filePath) {
    var dirname = path.dirname(filePath);
    if (fs.existsSync(dirname)) {
      return true;
    }
    ensureDirectoryExistence(dirname);
    fs.mkdirSync(dirname);
  }
  


  

  function parseXMLFromGCS(file) {
    const stream = file.createReadStream();
    const parser = new xml2js.Parser({ explicitArray: false });
    let data = '';
    stream.on('data', (chunk) => {
        data += chunk;
    });
  
    stream.on('end', () => {
        parser.parseString(data, (err, result) => {
            if (err) throw err;
            // Convert the result to an array if not already
            aircraftsData = Array.isArray(result.aircrafts.row) ? result.aircrafts.row : [result.aircrafts.row];
            console.log("Aircrafts data loaded");
        });
    });
  
    stream.on('error', (err) => {
        console.error('Error streaming the XML file:', err);
    });
  }
  


  function parseCSVFromGCS(file) {
    let results = [];
    const stream = file.createReadStream();
    console.log("Reading CSV from", file.name);

    stream.pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', () => {
            // Check the file name and assign data to the appropriate variable
            if (file.name === 'airports.csv') {
                airportsData = results;
                console.log("Airports data loaded");
            } else if (file.name === 'tickets.csv') {
                ticketsData = results; // Assuming you have a variable ticketsData
                console.log("Tickets data loaded");
            }
        });
}
  
  function parseJSONFromGCS(file) {
    const tempFilePath = `./temp/${file.name}`;
    ensureDirectoryExistence(tempFilePath); // Create directories if necessary
    return file.download({destination: tempFilePath})
      .then(() => {
        parseJSON(tempFilePath); // Your existing function
        // Optionally delete the temp file after parsing
      })
      .catch(err => console.error('Error downloading the JSON file:', err));
  }

  function parseJSON(filePath) {
    fs.readFile(filePath, 'utf8', (err, jsonString) => {
        if (err) {
            console.error("Error reading file:", err);
            return;
        }
        try {
            const data = JSON.parse(jsonString);
            // Process and extract data here
        } catch(err) {
            console.error('Error parsing JSON:', err);
        }
    });
}
  
function parseYAMLFromGCS(file) {
    const tempFilePath = `./temp/${file.name}`;
    ensureDirectoryExistence(tempFilePath); // Create directories if necessary
    return file.download({destination: tempFilePath})
        .then(() => {
            if (file.name === 'passengers.yaml') {
                parseYAML(tempFilePath, 'passengers');
            }
            // Add more conditions here if needed for other YAML files
        })
        .catch(err => console.error('Error downloading the YAML file:', err));
}

function parseYAML(filePath, type) {
    try {
        const fileContents = fs.readFileSync(filePath, 'utf8');
        const data = yaml.load(fileContents);
        if (type === 'passengers') {
            passengersData = data; 
            console.log("Passengers data loaded");
        }
    } catch (e) {
        console.error(e);
    }
}


function calculateDistance(lat1, lon1, lat2, lon2) {
    function toRadians(degree) {
        return degree * Math.PI / 180;
    }

    const R = 6371; // Radius of the Earth in kilometers
    const deltaLat = toRadians(lat2 - lat1);
    const deltaLon = toRadians(lon2 - lon1);
    lat1 = toRadians(lat1);
    lat2 = toRadians(lat2);

    const a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) +
              Math.cos(lat1) * Math.cos(lat2) *
              Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    return R * c; 
}


const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);

async function getFlightsData() {
    let flights = [];
    const years = await fs.promises.readdir('./temp/flights');

    for (const year of years) {
        const months = await fs.promises.readdir(`./temp/flights/${year}`);
        for (const month of months) {
            const filePath = `./temp/flights/${year}/${month}/flight_data.json`;
            const data = await readFile(filePath, 'utf8');
            const monthData = JSON.parse(data).map(flight => ({
                ...flight,
                month: month.padStart(2, '0'), // Ensure the month is two digits
                year: year
            }));
            flights = flights.concat(monthData);

            // Write the updated month data back to the file
            await writeFile(filePath, JSON.stringify(monthData, null, 2));
        }
    }

    // Optionally, you can write the combined flights array to a new file
    await writeFile('./temp/all_flights_with_date.json', JSON.stringify(flights, null, 2));

    return flights;
}


app.get('/api/airport/:name', (req, res) => {
    const airportName = req.params.name;
    console.log(airportName)
    console.log(airportsData[0])
    const airport = airportsData.find(ap => ap.name === airportName);
    if (airport) {
        res.json({ 
            name: airport.name,
            latitude: airport.lat,
            longitude: airport.lon 
        });
    } else {
        res.status(404).send('Airport not found');
    }
});


// New endpoint to get passengers by flight number
app.get('/api/passengers/:flightNumber', async (req, res) => {
    // Extract flight number from the request parameters
    const { flightNumber } = req.params;

    try {
        // Filter tickets data to find all tickets for the given flight number
        const ticketsForFlight = ticketsData.filter(ticket => ticket.flightNumber === flightNumber);

        // Find passenger information for each ticket
        const passengers = ticketsForFlight.map(ticket => {
            return passengersData.passengers.find(p => p.passengerID === ticket.passengerID);
        }).filter(p => p); // Filter out any undefined entries, in case a ticket has no matching passenger

        // Return the passengers data
        // You might want to include some error checking to make sure passengers is not empty
        res.json(passengers);
    } catch (error) {
        console.error('Error retrieving passengers for flight:', error);
        res.status(500).send('Error retrieving passengers for flight');
    }
});







app.get('/api/flights', async (req, res) => {
    const flights = await getFlightsData();
    try {
        let processedFlightsPromises = flights.map(async flight => {
            let aircraft = aircraftsData.find(ac => ac.aircraftID === flight.aircraftID);
            let originAirport = airportsData.find(ap => ap.airportIATA === flight.originIATA);
            let destinationAirport = airportsData.find(ap => ap.airportIATA === flight.destinationIATA);
            let flightPassengers = ticketsData
                .filter(ticket => ticket.flightNumber === flight.flightNumber) // Make sure the property name is correct
                .map(ticket => {
                    // Access the `passengers` array inside the `passengersData` object
                    return passengersData.passengers.find(p => p.passengerID === ticket.passengerID);
                })
                .filter(passenger => passenger != null); // Filter out any undefined or null entries

            // Calcula la edad promedio de los pasajeros
            let averageAge = flightPassengers.reduce((sum, passenger) => sum + getAge(passenger.birthDate), 0) / flightPassengers.length;
            // console.log("esta es la info del flight")
            // console.log(flightPassengers)
            // console.log(averageAge)
            // Calcula la distancia recorrida
            let distance = calculateDistance(originAirport.lat, originAirport.lon, destinationAirport.lat, destinationAirport.lon);
            return {
                ...flight,
                originAirport: originAirport.name,
                destinationAirport: destinationAirport.name,
                airline: flight.airline,
                averageAge: averageAge,
                distance: distance,
                aircraftName: aircraft ? aircraft.name : 'Unknown',
                passengerCount: flightPassengers.length
            };
        });

        // Ordena y paginación
        let processedFlights = await Promise.all(processedFlightsPromises);
        let pageNumber = parseInt(req.query.page, 10) || 1;
        let pageSize = 15;
        let paginatedFlights = paginate(processedFlights, pageNumber, pageSize)
        res.json({
            currentPage: pageNumber,
            pageSize: pageSize,
            totalItems: processedFlights.length,
            totalPages: Math.ceil(processedFlights.length / pageSize),
            flights: paginatedFlights
        });
        
    } catch (error) {
        console.error('Error in /api/flights:', error);
        res.status(500).send('Error processing flight data');
    }
});

function paginate(array, page_number, page_size) {
    return array.slice((page_number - 1) * page_size, page_number * page_size);
}

function getAge(birthDateString) {
    let spanishMonths = {
        'enero': 'January', 'febrero': 'February', 'marzo': 'March', 'abril': 'April',
        'mayo': 'May', 'junio': 'June', 'julio': 'July', 'agosto': 'August',
        'septiembre': 'September', 'octubre': 'October', 'noviembre': 'November', 'diciembre': 'December'
    };
    let dateParts = birthDateString.match(/(\d+) de ([^\s]+) de (\d+)/);
    if (!dateParts) return null; // or throw an error

    let day = dateParts[1];
    let month = spanishMonths[dateParts[2].toLowerCase()];
    let year = dateParts[3];
    let englishDateStr = `${month} ${day}, ${year}`;

    let today = new Date();
    let birthDate = new Date(englishDateStr);
    let age = today.getFullYear() - birthDate.getFullYear();
    let m = today.getMonth() - birthDate.getMonth();
    if (m < 0 || (m === 0 && today.getDate() < birthDate.getDate())) {
        age--;
    }
    return age;
}

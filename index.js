const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const { parse } = require('json2csv'); // To convert JSON rows into CSV format
const { Analytics } = require('@segment/analytics-node')
const crypto = require('crypto');

/* 
CHANGE BEFORE RUNNING
*/
const backfillFile = ''// Replace with your CSV file path
const filePath = `./CSVs/${backfillFile}`; 
const breakAfter = 100; // number of records to process in testing. If not testing, COMMENT OUT THIS LINE
const segmentWriteKey = ''; // Replace with your Segment write key
/* 
*/

// instantiate analytics
const analytics = new Analytics({ writeKey: segmentWriteKey }) 

// Function to send identify calls to Segment
async function sendToSegment(data) {
    const errorRows = [];
    const errorFilePath = path.resolve('error_rows.csv');

    // Prepare error file header
    if (!fs.existsSync(errorFilePath)) {
        fs.writeFileSync(errorFilePath, ''); // Create the file if it doesn't exist
    }

    let rowNum = 0;
    let recordsSent = 0;
    let recordsSkipped = 0;
    let uniqueEmails = new Set(); // Using a Set for efficient unique email tracking
    let numUsers = 0;
    let invalidEmails = 0;

    let currentOrder = null; // To track ongoing order
    let userIdHash = null; // Store the hashed user ID
    
    // Iterate through each row of the CSV data, processing orders and sending them to Segment
    for (const row of data) {

        // Break after processing breakAfter rows
        if(!isNaN(breakAfter)) {
            if (rowNum === breakAfter) break; 
        }

        // Log progress every 1000 rows
        if (rowNum % 1000 === 0) console.log(`Processing row number ${rowNum}...`);
        rowNum++;

        // Check if row signifies a new order
        if (row["Financial Status"] && row["Financial Status"].trim() !== "") {
            // Send the previous order if there is one
            if (currentOrder) {
                try {
                    await sendOrderToSegment(currentOrder, userIdHash);
                    recordsSent++;
                } catch (error) {
                    console.error(
                        'Error sending event for user:',
                        userIdHash,
                        error.message
                    );
                    errorRows.push(currentOrder);
                }
            }

            // Skip records not required
            if (row["Financial Status"] != "paid") {
                recordsSkipped++
                currentOrder = null
                continue;
            }

            // Skip invalid emails
            if (!row.Email || !isValidEmail(row.Email)) {
                invalidEmails++;
                recordsSkipped++;
                continue;
            }

            // Add emails to set to count unique users 
            if (!uniqueEmails.has(row.Email)) {
                uniqueEmails.add(row.Email);
                numUsers++;
            }

            // Start a new order
            userIdHash = crypto
                .createHash('md5')
                .update(row.Email)
                .digest('hex');
            const timestamp = new Date(row["Created at"]);
            if (isNaN(timestamp)) {
                console.log(`invalid date for ${row.Email}`);
                continue;
            }
            currentOrder = {
                userId: userIdHash,
                billingName: row["Billing Name"],
                email: row.Email,
                timestamp,
                properties: {
                    destination_country: row["Billing Country"],
                    tax_amount: +row["Taxes"],
                    promo_code: row["Discount Code"],
                    contentType: "product",
                    currency: row["Currency"],
                    shipping_amount: +row["Shipping"],
                    value: +row["Total"],
                    affiliation: "Shopify-chilisleep",
                    subtotal_price: +row["Subtotal"],
                    payment_method: row["Payment Method"],
                    order_id: row["Id"],
                    transaction_id: row["Id"],
                    amount: +row["Total"],
                    discount_amount: +row["Discount Amount"],
                    category: "Checkout",
                    revenue: +row["Total"],
                    total: +row["Total"],
                    backfill_file: backfillFile,
                    products: [
                        {
                            price: +row["Lineitem price"],
                            sku: row["Lineitem sku"],
                            amount: +row["Lineitem price"],
                            quantity: +row["Lineitem quantity"],
                            item: row["Lineitem sku"],
                            name: row["Lineitem name"]
                        }
                    ]
                }
            };
        } else if (currentOrder && row.Email) {
            // Add additional line items to the current order
            currentOrder.properties.products.push({
                price: +row["Lineitem price"],
                sku: row["Lineitem sku"],
                amount: +row["Lineitem price"],
                quantity: +row["Lineitem quantity"],
                item: row["Lineitem sku"],
                name: row["Lineitem name"]
            });
        } else {
            // Skip invalid rows
            recordsSkipped++;
            continue;
        }

    }

    // Send the last order if it exists
    if (currentOrder) {
        try {
            await sendOrderToSegment(currentOrder, userIdHash);
            recordsSent++;
        } catch (error) {
            console.error(
                'Error sending event for user:',
                userIdHash,
                error.message
            );
            errorRows.push(currentOrder);
        }
    }

    // Log summary to the console
    console.log(".")
    console.log(".")
    console.log(".")
    console.log(`SUMMARY for ${backfillFile}:`);
    console.log(`Total rows: ${rowNum}`);
    console.log(`Sent ${recordsSent} records to Segment`);
    console.log(`Skipped ${recordsSkipped} records`);
    console.log(`Number of unique users: ${numUsers}`);
    console.log(`Number of invalid emails: ${invalidEmails}`);

    // Write all error rows to the CSV file after the loop
    if (errorRows.length > 0) {
        try {
            // Convert error rows to CSV
            const csv = parse(errorRows, { header: true });
            fs.appendFileSync(errorFilePath, csv + '\n');
            console.log(`Error rows written to ${errorFilePath}`);
        } catch (err) {
            console.error('Failed to write error rows to CSV:', err.message);
        }
    }
}

// Read and process the CSV file
function processCSV(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];

        fs.createReadStream(filePath)
            .pipe(
                csv({
                    mapHeaders: ({ header }) => formatHeader(header),
                })
            )
            .on('data', (row) => {
                results.push(row);
            })
            .on('end', () => resolve(results))
            .on('error', (error) => reject(error));
    });
}

// Helper function to check if email is valid
function isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}

// Function to convert headers to lowercase with underscores replacing spaces
function formatHeader(header) {
    // return header.trim().toLowerCase().replace(/\s+/g, '_');
    return header;
}

// Helper function to send an order to Segment
async function sendOrderToSegment(order, userIdHash) {
    await analytics.track({
        userId: userIdHash,
        event: 'Order Completed',
        timestamp: order.timestamp,
        properties: order.properties,
        context: {
            traits: {
                name: order.billingName,
                email: order.email
            }
        }
    });
}


// Main function to handle the entire process
async function main() {
    
    try {
        console.log('Processing CSV...');
        const data = await processCSV(filePath);
        console.log('CSV processing completed. Sending data to Segment...');
        await sendToSegment(data);
        await analytics.closeAndFlush()
        console.log('Data sent to Segment successfully.');
    } catch (error) {
        console.error('An error occurred:', error.message);
    }
}

main();
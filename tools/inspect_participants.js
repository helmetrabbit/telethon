
const fs = require('fs');
const path = require('path');

const src = path.join(__dirname, '../data/exports/telethon_bd_web3.json');

try {
  const data = JSON.parse(fs.readFileSync(src, 'utf-8'));
  console.log('Participants found:', data.participants ? data.participants.length : 0);
  
  if (data.participants && data.participants.length > 0) {
    console.log('Sample Participant Keys:', Object.keys(data.participants[0]));
    
    // Check for bio-like fields in first 10
    let bioCount = 0;
    for (let i = 0; i < Math.min(100, data.participants.length); i++) {
        const p = data.participants[i];
        if (p.about || p.bio || p.description) {
            bioCount++;
            console.log(`User ${p.user_id}: about="${p.about}", bio="${p.bio}"`);
        }
    }
    console.log(`\nFound ${bioCount} bios in first 100 participants.`);
  }
} catch (e) {
  console.error(e);
}

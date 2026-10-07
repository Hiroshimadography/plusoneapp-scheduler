import {createCipheriv,createDecipheriv,randomBytes} from 'node:crypto';
const aad=Buffer.from('plusoneapp-scheduler:v1');
export function encryptState(state,key){
 const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',Buffer.from(key,'base64'),iv);cipher.setAAD(aad);
 const bytes=Buffer.concat([cipher.update(JSON.stringify(state),'utf8'),cipher.final()]);
 return {version:1,iv:iv.toString('base64'),tag:cipher.getAuthTag().toString('base64'),ciphertext:bytes.toString('base64')};
}
export function decryptState(envelope,key){
 if(envelope.version!==1)throw Error('Unsupported encrypted state');
 const decipher=createDecipheriv('aes-256-gcm',Buffer.from(key,'base64'),Buffer.from(envelope.iv,'base64'));decipher.setAAD(aad);decipher.setAuthTag(Buffer.from(envelope.tag,'base64'));
 return JSON.parse(Buffer.concat([decipher.update(Buffer.from(envelope.ciphertext,'base64')),decipher.final()]).toString('utf8'));
}

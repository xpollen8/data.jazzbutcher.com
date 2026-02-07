const Db = require('mywrap');
const mysql = require('mysql2');

let db;
let db_FEEDBACK;

const censorEmail = (str) => {
	if (!str) return;
	const [ addr, fqdn ] = str?.split('@');
	if (!fqdn) return str;
	const parts = fqdn?.split('.');
	const top = parts.pop();
	const domain = parts.join('.');
	const blank = new Array(domain.length + Math.floor(Math.random() * 4)).join( '.' );
	return addr + '@' + blank + '.' + top;
}

const queries = [
	{ noun: "gigs", query: "select * from gig where isdeleted IS NULL order by datetime" },
	{ noun: "gigsongs", query: "select * from gigsong order by datetime, type, setnum, ordinal" },
	{ noun: "gigtexts", query: "select * from gigtext order by datetime" },
	{ noun: "gigmedias", query: "select * from gigmedia" },
	{ noun: "performances", query: "select * from performance order by datetime" },
	{ noun: "presses", query: "select *, LENGTH(body) - LENGTH(REPLACE(body, ' ', '')) as bodycount from press" },
	{ noun: "medias", query: "select * from media order by dtcreated" },
	{ noun: "lyrics", query: "select * from lyrics order by title" },

	{ noun: "feedbacks", query: "select * from feedback where isdeleted <> 'T' order by dtcreated desc" },
	{ noun: "feedback", query: 'select * from feedback where isdeleted <> "T" and uri = "{{value}}" order by dtcreated desc' },
	{ noun: "recent_feedback", query: "select * from feedback where isdeleted <> 'T' order by dtcreated desc limit 5" },

	{ noun: "release_video_by_project", key: 'project', query: "select * from media where ? and type='video' order by collection, ordinal" },
	{ noun: "live_video_by_project", key: 'project', query: "select * from media where ? and type='video' and datetime <> '0000-00-00 00:00:00' order by collection, ordinal" },
];

const unUTC = (timestampStr) => {
	try {
		//return new Date(new Date(timestampStr)?.getTime() - (new Date(timestampStr)?.getTimezoneOffset() * 60 * 1000))?.toISOString()?.replace(/T/, ' ')?.replace(/Z/, '')?.substr(0, 19);
		return new Date(new Date(timestampStr)?.getTime() - (new Date(timestampStr)?.getTimezoneOffset() * 60 * 1000))?.toISOString()?.replace(/T/, ' ')?.replace(/Z/, '')?.substr(0, 19)?.replace(/ 00:00:00/, '');
	} catch (e) {
		// return as-is
		return timestampStr;
	}
}

const removeHTML = (str) => {
	const deParagraphed = str
		?.replace(/&nbsp;/ig, ' ')
		?.replace(/<BR>/ig, '<br/>') // <BR> => <br/>
		?.replace(/<p>/ig, '<br/>') // <p> => <br/>
		?.replace(/<p([^>]+)>/ig, '<br/>')  // <p.....> => <br/>
		?.replace(/<\/p>/ig, '<br/>') // </p> => <br/>
		?.trim();
	// need to leave <br/ tags intact
	const unlinked = deParagraphed
		?.replace(/<p>/gi, '<p\/>')
		?.replace(/<br>/gi, '<br\/>')
		?.replace(/(<(?!br\/)([^>]+)>)/ig, '')
		?.replace(/\s\s+/g, ' ')	// collapse all spaces into one
		?.replace(/(<([^>]+)>)/ig, '')
		?.trim();
	return unlinked;
}

const pruneRow = (row) => {
	try {
	// remove empty attributes from data structure
	const ret = {};
	Object.keys(row).forEach(index => {
		// keep explicit '0' values
		if ((!row[index] && row[index] !== 0) || row[index] === '0000-00-00 00:00:00' || row[index] === 'NULL') {
			//if (index === 'author') console.log("DROP", index);
		} else if (index === 'added' || index === 'datetime' || index === 'dtadded' || index === 'dtgig' || index === 'dtpublished' || index === 'credit_date' || index === 'dtcreated') {
			ret[index] = unUTC(row[index]);
		} else {
			if (['dtadded','credit_date'].includes(index) && !row['added']) {
				/*
					add 'added' data if not already exists by that name
				 */
				row['added'] = row[index];
			}
			if (index === 'body' && row[index]?.length) {
				/*
					body fields may contain valid HMTL
				 */
				ret[index] = row[index]
					?.replace(/&nbsp;/ig, ' ')
					?.replace(/&quot;/ig, '"')
					?.replace(/ ,/g, ',')
					?.replace(/<p>/gi, '<p\/>')
					?.replace(/<br>/gi, '<br\/>')
					?.replace(/\s\s+/g, ' ')	// collapse all spaces into one
			} else {
				if (index === 'summary' || index === 'caption' || index === 'image_caption') {
					/*
						get rid of HTML from these fields
					 */
					ret[index] = removeHTML(row[index]);
				} else {
					ret[index] = row[index];
				}
			}
		}
	});
	return ret;
	} catch (e) {
		//console.log("ERR", e);
	}
}

const doQuery = async (noun, key, type, value) => {
	try {
		const obj = queries.find(q => q.noun === noun);
		if (!obj) {
			return { noun, key, value, error: 'object not found' }
		}
		key = key || obj.key || 'id';
		//console.log("OBJ", { noun, key, value, obj });
		let sql = JSON.parse(JSON.stringify(obj.query));
		if (sql.indexOf(`{{value}}`) > 0) {
			if (value) {
				if (typeof value === 'string') {
					value =  value.replace('+',' ');
				} else {
					value = JSON.parse(JSON.stringify(value));
				}
				sql = sql.replace(/{{value}}/g, value);
			} else {
				return { noun, key, value, error: "ID REQUIRED" };
			}
		}
		value = value?.replace(/"/g, '');
		const Q = mysql.format(sql, [ { [key]: value } ]);
		//console.log("Q", Q, value);

		return await db.query(Q)
			.then(async results => {
				results = results?.map(r => pruneRow({
					...r,
					// obscure email addresses
					who: (noun?.includes('feedback')) ? censorEmail(r?.who) : r?.who
				}));
				return { noun, key, value, numResults: results.length, results };
			})
			.catch(error => {
				return { noun, key, value, error };
			});
	} catch (e) {
		console.log("ERROR", e);
	}
}

const handler = async (req, res) => {
	try {
		const { path = [] } = req.query;
		const method = req.method;
		let [ noun, key, type, value ] = path;
		if (key === 'exact') {	// will override path-based queries
			value = '/' + path.slice(2).join('/') || ''; // [ 'feedback', 'exact', 'lyrics', 'sea_madness.html' ] -> 'lyrics/sea_madness.html'
		} else if (!type) {					// gig/1974
			value = key;
			type = 'is';
			key = null;
		} else if (!value) {	//  gig/venue/Zabo
			value = type;
			type  = 'is';
		}
		//console.log("PATH", path, [ key, type, value ]);
		if (method === 'DELETE' || method === 'POST' || noun === 'feedback_delete')  {
			if (!db_FEEDBACK) {
				db_FEEDBACK = await (new Db({
					host: process.env['JBC_MYSQL_HOST'],
					user: process.env['JBC_MYSQL_USER_FEEDBACK'],
					database: process.env['JBC_MYSQL_DATABASE'],
					password: process.env['JBC_MYSQL_PASSWORD_FEEDBACK'],
					connectionLimit: process.env['JBC_MYSQL_CONNECTIONLIMIT'],
					queueLimit: process.env['JBC_MYSQL_QUEUELIMIT'],
				})).start();
			}
			if (noun === 'gigsong_edit')  {
				const deletes = [];
				const inserts = [];
				const edits = await Promise.all(req.body?.edits?.map(async (e) => {
					return await db_FEEDBACK.query('update gigsong set ? where gigsong_id = ?',
						[
							e,
							e.gigsong_id,
						]);
					})
				);

				res.json({ edits, deletes, inserts });
			} else if (noun === 'feedback_delete')  {
				const resX = await db_FEEDBACK.query('update feedback set isdeleted = ? where feedback_id = ?',
					[
						"T",
						value,
					]);
				//console.log("RES", resX);
				res.json(rets);
			} else {
				const { session, host, feedback_id, uri, subject, who, whence, comments, isdeleted = 'F' } = req.body;
				//console.log("POST", { session, host, path, noun, key, type, value, body: req.body });
				switch (noun) {
					case 'feedback_by_page_new': {
						const resX = await db_FEEDBACK.query('insert IGNORE into `feedback` set `session` = ?, `host` = ?, `feedback_id` = NULL, `uri` = ?, `subject` = ?, `dtcreated` = NOW(), `who` = ?, `whence` = ?, `comments` = ?, `isdeleted` = ?',
							[
								session,
								host,
								uri,
								subject,
								who || 'No Email Given',
								whence || 'No Location Given',
								comments,
								isdeleted || 'F'
							]);
						return res.json(resX);
					}
					break;
					case 'feedback_by_page_reply': {
						if (!feedback_id) { return res.json({ error: 'missing: feedback_id' }); }
						const resX = await db_FEEDBACK.query('insert IGNORE into `feedback` set `session` = ?, `host` = ?, `feedback_id` = NULL, `parent_id` = ?, `uri` = ?, `subject` = ?, `dtcreated` = NOW(), `who` = ?, `whence` = ?, `comments` = ?',
							[
								session,
								host,
								feedback_id,
								uri,
								subject,
								who || 'No Email Given',
								whence || 'No Location Given',
								comments
							]);
						//console.log("XX", resX);
						return res.json(resX);
					}
					break;
					default:
						return res.json({ error: `unknown: ${noun}` });
				}
			}
		} else if (method === 'GET')  {
			if (!db) {
				db = await (new Db({
					host: process.env['JBC_MYSQL_HOST'],
					user: process.env['JBC_MYSQL_USER'],
					database: process.env['JBC_MYSQL_DATABASE'],
					password: process.env['JBC_MYSQL_PASSWORD'],
					connectionLimit: process.env['JBC_MYSQL_CONNECTIONLIMIT'],
					queueLimit: process.env['JBC_MYSQL_QUEUELIMIT'],
				})).start();
			}
			const ret = await doQuery(noun, key, type, value);
			//console.log("RET", ret);
			res.json(ret);
		}
	} catch (e) {
		console.log("ERROR", e);
		res.status(400).send(e);
	}
}

export default handler;

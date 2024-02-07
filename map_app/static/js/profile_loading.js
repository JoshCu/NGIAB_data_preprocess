var profile_loader = {};
profile_loader.id = "profile_loader";
profile_loader.doms = {};
profile_loader.ready = false;
profile_loader.uuid = 0;
profile_loader.timingdata = {};
profile_loader.timingdata.starts = {};
profile_loader.timingdata.intervals = [];
profile_loader.timingdata.total_time = 0;
profile_loader.timingdata.count = 0;
profile_loader.timingdata.aborted = 0;
profile_loader.timingdata.failed = 0;
profile_loader.timingdata.changed = false;
profile_loader.debugstr = (obj, depth=0) => {
    switch (typeof obj) {
        case "string":
            return obj;
        case "number":
            return obj.toString();
        case "object":
            if (obj instanceof Array) {
                var outstr = "[";
                const multiline = obj.length > 5;
                if (multiline) {
                    outstr += "\n" + " ".repeat(depth);
                }
                for (let i = 0; i < obj.length; i++) {
                    const element = obj[i];
                    if (i > 0) {
                        outstr += ", ";
                        if (multiline) {
                            outstr += "\n" + " ".repeat(depth);
                        }
                    }
                    outstr += profile_loader.debugstr(element, depth + 1);
                }
                if (multiline) {
                    outstr += "\n" + " ".repeat(depth);
                }
                outstr += "]";
                return outstr;
            }
            else {
                var outstr = "{";
                var keycount = Object.keys(obj).length;
                const multiline = keycount > 5;
                for (const key in obj) {
                    if (Object.hasOwnProperty.call(obj, key)) {
                        const element = obj[key];
                        if (multiline) {
                            outstr += "\n" + " ".repeat(depth);
                        }
                        outstr += profile_loader.debugstr(element, depth + 1);
                        outstr += ", ";
                    }
                }
                if (keycount > 1) {
                    outstr = outstr.slice(0,outstr.length-2);
                }
                if (multiline) {
                    outstr += "\n" + " ".repeat(depth);
                }
                outstr += "}";
                return outstr
            }
        default:
            return "[" + typeof obj + ":" + obj + "]"
    }
}
profile_loader.get_uuid = () => {
    profile_loader.uuid += 1;
    return profile_loader.uuid - 1;
}
profile_loader.init = () => {
    var overhead = document.createElement("div");
    overhead.id = profile_loader.id + "_overhead"
    overhead.style.position = "fixed";
    overhead.style.zIndex = 500;
    overhead.style.right = "0";
    overhead.style.top = "0";
    overhead.style.width = "20%";
    overhead.style.height = 12 * 6;
    // overhead.style.color = "red";
    // overhead.style.fill = "red";
    // overhead.style.fillOpacity = 100
    overhead.style.backgroundColor = "lightgrey";
    overhead.style.borderWidth = "1px";
    overhead.style.borderColor = "black";
    overhead.style.borderStyle = "solid";
    overhead.innerHTML = "";
    profile_loader.doms.overhead = overhead;
    document.body.appendChild(overhead);
    profile_loader.ready = true;
}
profile_loader.start_loading = (evnt) => {
    var obj = evnt.tile;
    if (obj.id == ""||!("id" in obj)){
        obj.id = profile_loader.get_uuid();
    }
    profile_loader.timingdata.starts[obj.id] = Date.now()
    profile_loader.timingdata.changed = true;
}
profile_loader.stop_loading = (evnt) => {
    var obj = evnt.tile;
    var timingdata = profile_loader.timingdata;
    if (obj.id in timingdata.starts) {
        dt = Date.now()-timingdata.starts[obj.id];
        timingdata.intervals.push(dt);
        timingdata.count += 1;
        timingdata.total_time += dt;
        delete timingdata.starts[obj.id];
        timingdata.changed = true;
    }
    else {
        throw EvalError(obj.id + " finished loading before started?\n timingdata dump: \n" + profile_loader.debugstr(timingdata));
    }
}
profile_loader.abort_load = (evnt) => {
    var obj = evnt.tile;
    var timingdata = profile_loader.timingdata;
    if (obj.id in timingdata.starts) {
        delete timingdata.starts[obj.id];
        timingdata.aborted += 1;
    }
}
profile_loader.cleanup = () => {
    var timingdata = profile_loader.timingdata;
    var cleantime = Date.now();
    var deletion = [];
    for (const id in timingdata.starts) {
        if (Object.hasOwnProperty.call(timingdata.starts, id)) {
            const element = timingdata.starts[id];
            if ((cleantime - element)/1000 > 10) {
                deletion.push(id);
            }
        }
    }
    deletion.forEach(id => {
        delete timingdata.starts[id];
        timingdata.failed += 1;
    });
}
profile_loader.update = async () => {
    if (!profile_loader.ready) {
        profile_loader.init();
    }
    var timingdata = profile_loader.timingdata;
    if (!timingdata.changed) {}
    else {
        timingdata.changed = false;
        var div = document.getElementById(profile_loader.id + "_overhead");
        div.innerHTML = "";
        const num_incompl = Object.keys(timingdata.starts).length;
        if (num_incompl > 0) {
            profile_loader.cleanup()
        }
        div.innerHTML += "Incomplete: " + num_incompl + "<br>"
        div.innerHTML += "Aborted: " + timingdata.aborted + "<br>"
        div.innerHTML += "Failed: " + timingdata.failed + "<br>"
        div.innerHTML += "Complete: " + timingdata.count.toString() + "<br>"
        div.innerHTML += "Total Time: " + (timingdata.total_time/1000).toFixed(3) + "<br>"
        div.innerHTML += "Average: " + (timingdata.total_time/1000/timingdata.count).toFixed(3) + "<br>"
    }
}

setInterval(profile_loader.update, 1000);

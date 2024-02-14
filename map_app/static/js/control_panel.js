control_panel = {}
control_panel.id = "control_panel"
control_panel.initialized = false
control_panel.inits = {}
control_panel.settings = {}
control_panel.settings.layers = {
    streetmap: {
        toggle: {type: "bool", default: true},
        opacity: {type: "percent", default: 1}
    },
    waterbasins: {
        toggle: {type: "bool", default: true},
        opacity: {type: "percent", default: 1}
    },
    boundaries_of_vpus: {
        toggle: {type: "bool", default: true},
        opacity: {type: "percent", default: 1}
    }
}
control_panel.settings.geometries = {
    selected_wb_layer: {
        toggle: {type: "bool", default: true},
        style: {
            type: "group",
            opacity: {type: "percent", default: 0.6},
            color: {type: "picker", default: "#eb34d8"},
            fillColor: {type: "picker", default: "#e0abff"},
            fillOpacity: {type: "percent", default: 0.2}
        }
    },
    merged_geometry: {
        toggle: {type: "bool", default: true},
        style: {
            type: "group",
            opacity: {type: "percent", default: 0.6},
            color: {type: "picker", default: "#3388ff"},
            fillColor: {type: "picker", default: "#3388ff"},
            fillOpacity: {type: "percent", default: 0.2}
        }
    },
    merged_tolines: {
        toggle: {type: "bool", default: true},
        style: {
            type: "group",
            opacity: {type: "percent", default: 0.5},
            color: {type: "picker", default: "#FF0000"},
            weight: {type: "number", default: 1}
        }
    },
    merged_from_nexus: {
        toggle: {type: "bool", default: true},
        style: {
            type: "group",
            opacity: {type: "percent", default: 0.8},
            color: {type: "picker", default: "#00FF00"},
            weight: {type: "number", default: 2}
        }
    },
    nexus_circles: {
        toggle: {type: "bool", default: true},
        length: {type: "float", default: 0.25},
        style: {
            type: "group",
            opacity: {type: "percent", default: 0.8},
            color: {type: "picker", default: "#FFFF00"},
            fillColor: {type: "picker", default: "#FFFF00"},
            fillOpacity: {type: "percent", default: 0.2},
            weight: {type: "number", default: 2}
        }
    }
}
control_panel.values = {}
control_panel.init_values = function (settings, keystr="") {
    for (const key in settings) {
        if (Object.hasOwnProperty.call(settings, key)) {
            const element = settings[key];
            if (key == "type") {
                continue
            }
            var cur_key = keystr+"."+key
            if (Object.hasOwnProperty.call(element, "type")) {
                if (element["type"] == "group") {
                    control_panel.init_values(settings[key], cur_key)
                }
                else {
                    control_panel.values[cur_key] = element["default"]
                }
            }
            else {
                control_panel.init_values(settings[key], cur_key)
            }
        }
    }
    if (keystr == "") {
        control_panel.inits["values"] = "done";
        all_inits = ["settings", "dom", "values"]
        if (all_inits.every(init => control_panel.inits[init] == "done")) {
            control_panel.initialized = true;
        }
    }
}
control_panel.init_values(control_panel.settings)
console.log(control_panel.values)
control_panel.dom = {}
control_panel.dom_config = {}
control_panel.dom_callbacks = {}
control_panel.state = {}
control_panel.dom_id_prefix = "control_panel_"
control_panel.utility = {}
control_panel.utility.create_button = function (id, text, onclick) {
    var button = document.createElement("button")
    button.id = id
    button.innerHTML = text
    button.onclick = onclick
    return button
}
control_panel.utility.create_input = function (id, type, value, onchange) {
    var input = document.createElement("input")
    input.id = id
    input.type = type
    input.value = value
    if (type == "checkbox") {
        input.checked = value
        input.defaultChecked = value
    }
    else {
        input.defaultValue = value
    }
    input.onchange = onchange
    return input
}
control_panel.utility.create_label = function (id, text) {
    var label = document.createElement("label")
    label.id = id
    label.innerHTML = text
    return label
}
control_panel.utility.create_slider = function (id, min, max, value, onchange) {
    var slider = document.createElement("input")
    slider.id = id
    slider.type = "range"
    slider.step = "0.01"
    slider.min = min
    slider.max = max
    slider.value = value
    slider.defaultValue = value
    slider.onchange = onchange
    return slider
}
control_panel.utility.setup_callback = function (id, callback) {
    if (id in control_panel.dom_callbacks) {
        control_panel.dom_callbacks[id].push(callback);
    }
    else {
        control_panel.dom_callbacks[id] = [callback];
    }
}
control_panel.utility.trigger_callback = function (id) {
    if (id in control_panel.dom_callbacks) {
        console.log("Callback: "+id)
        control_panel.dom_callbacks[id].forEach(callback => {
            callback()
        });
    }
}
control_panel.init_dom = function () {
    // Create the basic control panel structure with the necessary elements, 
    //before adding setting-related elements
    var container = document.createElement("div")
    container.id = control_panel.dom_id_prefix+"container"
    control_panel.dom.container = container
    control_panel.dom_config.container = {}
    control_panel.dom_config.container.minimized_attrs = {
        position: "fixed",
        style: {
            position: "fixed",
            top: "0",
            right: "0",
            width: "30%",
            height: "10%",
            backgroundColor: "rgba(255, 255, 255, 0.8)",
            zIndex: "1000",
            overflow: "scroll",
            border: "1px solid black",
            display: "flex",
            opacity: "0.2"
        }
    }
    control_panel.dom_config.container.maximized_attrs = {
        position: "fixed",
        style: {
            position: "fixed",
            top: "0",
            right: "0",
            width: "70%",
            height: "70%",
            backgroundColor: "rgba(255, 255, 255, 0.8)",
            zIndex: "1000",
            overflow: "scroll",
            border: "1px solid black",
            display: "flex",
            opacity: "0.8"
        }
    }
    control_panel.dom_config.activate = function (elem, attrs) {
        for (const key in attrs) {
            if (Object.hasOwnProperty.call(attrs, key)) {
                const element = attrs[key];
                if (key == "style") {
                    for (const style_key in element) {
                        if (Object.hasOwnProperty.call(element, style_key)) {
                            const style_value = element[style_key];
                            elem.style[style_key] = style_value;
                        }
                    }
                }
                else {
                    elem[key] = element;
                }
            }
        }
    }
    control_panel.dom_config.activate(control_panel.dom.container, control_panel.dom_config.container.minimized_attrs)
    document.body.appendChild(control_panel.dom.container);
    control_panel.state.minimized = true;
    // Create the button to toggle the control panel
    var button_container = document.createElement("div")
    button_container.id = control_panel.dom_id_prefix+"button_container"
    control_panel.dom.button_container = button_container
    var button_attrs = {
        position: "absolute",
        style: {
            position: "absolute",
            top: "0",
            left: "0",
            zIndex: "1000",
            height: "30px",
            width: "100%",
            backgroundColor: "lightgray",
        }
    }
    control_panel.dom_config.activate(control_panel.dom.button_container, button_attrs)
    control_panel.dom.container.appendChild(control_panel.dom.button_container)
    // control_panel.utility.setup_callback("toggle_button", toggle_func);
    var toggle_func = function () {
        if (control_panel.state.minimized) {
            control_panel.dom_config.activate(control_panel.dom.container, control_panel.dom_config.container.maximized_attrs)
        }
        else {
            control_panel.dom_config.activate(control_panel.dom.container, control_panel.dom_config.container.minimized_attrs)
        }
        control_panel.state.minimized = !control_panel.state.minimized;
        control_panel.utility.trigger_callback("toggle_button");
    }
    var toggle_info = {
        id: control_panel.dom_id_prefix+"toggle_button",
        text: "Toggle Control Panel",
        onclick: toggle_func
    }
    control_panel.dom.toggle_button = control_panel.utility.create_button(toggle_info.id, toggle_info.text, toggle_info.onclick);
    control_panel.dom.button_container.appendChild(control_panel.dom.toggle_button);
    var toggle_attrs = {
        position: "absolute",
        style: {
            position: "absolute",
            top: "0",
            left: "0",
            zIndex: "1000",
            height: "30px",
            width: "150px",

        }
    }
    control_panel.dom_config.activate(control_panel.dom.toggle_button, toggle_attrs)
    control_panel.inits["dom"] = "done";
    all_inits = ["settings", "dom", "values"]
    if (all_inits.every(init => control_panel.inits[init] == "done")) {
        control_panel.initialized = true;
    }
}
control_panel.dom_config.setup_toggle_listen = function (element) {
    //For settings related elements, listen for the window toggle event, and hide/show the element accordingly
    var toggle_func = function () {
        if (control_panel.state.minimized) {
            element.style.display = "none";
        }
        else {
            element.style.display = "block";
        }
    }
    control_panel.utility.setup_callback("toggle_button", toggle_func);
}
//subcategory creation: create a div with a min/max toggle button, and a title
//return the div. child settings will be appended to this div as children
control_panel.dom_config.create_settings_subcategory = function (title, id, depth=0) {
    var subcat_container = document.createElement("div")
    subcat_container.id = id + "_container"
    var backgroundColor = "lightgray"
    var backgroundColor2 = "darkgray"
    if (depth % 2 == 0) {
        backgroundColor = "darkgray"
        backgroundColor2 = "lightgray"
    }
    var subcat_container_attrs = {
        position: "relative",
        style: {
            position: "relative",
            width: "calc(100%-2px)",
            display: "flex",
            flexFlow: "column",
            backgroundColor: backgroundColor,
            border: "1px solid black",
            margin: "1px"
        }
    }
    var subcat_control = document.createElement("div")
    subcat_control.id = id + "_control"
    var subcat_control_attrs = {
        position: "absolute",
        style: {
            position: "relative",
            width: "100%",
            height: "auto",
            display: "flex",
            flexDirection: "row",
            backgroundColor: backgroundColor2
        }
    }
    control_panel.dom_config.activate(subcat_container, subcat_container_attrs)
    control_panel.dom_config.activate(subcat_control, subcat_control_attrs)
    var subcategory_title = document.createElement("div")
    subcategory_title.id = id + "_title"
    subcategory_title.innerHTML = title
    subcat_control.appendChild(subcategory_title)
    var subcategory_toggle = document.createElement("button")
    subcategory_toggle.id = id + "_toggle"
    subcategory_toggle.innerHTML = "Toggle"
    subcategory_toggle.onclick = function () {
        var subcat = document.getElementById(id)
        if (subcat.style.display == "none") {
            subcat.style.display = "flex"
        }
        else {
            subcat.style.display = "none"
        }
    }
    subcat_control.appendChild(subcategory_toggle)
    subcat_container.appendChild(subcat_control)
    var subcategory = document.createElement("div")
    subcategory.id = id
    var subcategory_attrs = {
        position: "relative",
        style: {
            position: "relative",
            width: "calc(100%-1px)",
            display: "flex",
            flexDirection: "column",
            backgroundColor: backgroundColor2,
            flex: "1"
        }
    }
    control_panel.dom_config.activate(subcategory, subcategory_attrs)
    subcat_container.appendChild(subcategory)
    
    return [subcat_container, subcategory]
}
control_panel.dom_config.create_setting_element = function (id, type, value, onchange) {
    var setting_row = document.createElement("div")
    setting_row.id = id + "_row"
    var setting_row_attrs = {
        position: "relative",
        style: {
            position: "relative",
            width: "100%",
            display: "inline-flex",
            flexDirection: "row",
            backgroundColor: "white"
        }
    }
    control_panel.dom_config.activate(setting_row, setting_row_attrs)
    var setting_label = control_panel.utility.create_label(id + "_label", id)
    setting_row.appendChild(setting_label)
    var setting_element = null
    if (type == "bool") {
        setting_element = control_panel.utility.create_input(id, "checkbox", value, onchange)
    }
    else if (type == "percent") {
        setting_element = control_panel.utility.create_slider(id, 0, 1, value, onchange)
        
    }
    else if (type == "number") {
        setting_element = control_panel.utility.create_input(id, "number", value, onchange)
    }
    else if (type == "picker") {
        setting_element = control_panel.utility.create_input(id, "color", value, onchange)
    }
    else if (type == "float") {
        setting_element = control_panel.utility.create_input(id, "number", value, onchange)
        setting_element.step = "0.01"
    }
    setting_element.style.position = "relative"
    setting_element.style.right = "0"
    setting_element.style.marginLeft = "auto"
    setting_row.appendChild(setting_element)
    return setting_row

}
control_panel.utility.get_setting_definition = function (key) {
    var keys = key.split(".").slice(1);
    var setting = control_panel.settings
    for (var i = 0; i < keys.length; i++) {
        setting = setting[keys[i]]
    }
    return setting
}
control_panel.utility.get_setting_value = function (key, setting_obj=null) {
    //If key is an exact path, give the value of the setting
    //If key is a partial path, give an object of the keys within the path
    var keys = key.split(".");
    if (keys[0] == "") {
        keys = keys.slice(1);
    }
    if (setting_obj == null) {
        var result = {};
        for (const setting in control_panel.values) {
            var setting_keys = setting.split(".").slice(1);
            if (keys.length > setting_keys.length) {
                continue;
            }
            else if (key == setting) {
                return control_panel.values[setting];
            }
            var match = true;
            for (var i = 0; i < keys.length; i++) {
                if (keys[i] != setting_keys[i]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                var remaining_keys = setting_keys.slice(keys.length)
                var pos = result
                for (var i = 0; i < remaining_keys.length; i++) {
                    if (!(remaining_keys[i] in pos)) {
                        if (i == remaining_keys.length - 1) {
                            pos[remaining_keys[i]] = control_panel.values[setting]
                        }
                        else {
                            pos[remaining_keys[i]] = {}
                        }
                    }
                    pos = pos[remaining_keys[i]]
                }
            }
        }
        return result
    }
    else {
        var pos = setting_obj
        for (var i = 0; i < keys.length; i++) {
            if (!(keys[i] in pos)) {
                return null
            }
            pos = pos[keys[i]]
        }
        return pos
    }
}
control_panel.setup_settings = function () {
    // Create the settings related elements
    var settings_container = document.createElement("div")
    settings_container.id = control_panel.dom_id_prefix+"settings_container"
    control_panel.dom.settings_container = settings_container
    control_panel.dom.container.appendChild(control_panel.dom.settings_container)
    var settings_attrs = {
        position: "absolute",
        style: {
            position: "absolute",
            top: "30px",
            left: "0",
            zIndex: "1000",
            height: "calc(100% - 30px)",
            width: "100%",
            backgroundColor: "lightgray",
            display: "flex",
            flexDirection: "column",
            overflow: "scroll"
        }
    }
    control_panel.dom_config.activate(control_panel.dom.settings_container, settings_attrs)
    control_panel.dom_config.setup_toggle_listen(control_panel.dom.settings_container)
    control_panel.utility.trigger_callback("toggle_button");
    // Create the settings
    var subcats = {}
    for (const key in control_panel.values) {
        if (Object.hasOwnProperty.call(control_panel.values, key)) {
            const element = control_panel.values[key];
            var keys = key.split(".").slice(1);
            //between 3 and 4 keys. Most settings are 3 keys deep, but some, such as style-related settings, are 4 keys deep
            //Every key before the last one is a subcategory
            var keynum = keys.length - 1
            var subcat_ids = []
            for (var i = 0; i < keynum; i++) {
                subcat_ids.push(keys.slice(0, i+1).join("_"))
            }
            for (var i = 0; i < keynum; i++) {
                if (!(subcat_ids[i] in subcats)) {
                    subcats[subcat_ids[i]] = control_panel.dom_config.create_settings_subcategory(keys[i], subcat_ids[i], i)
                    if (i == 0) {
                        control_panel.dom.settings_container.appendChild(subcats[subcat_ids[i]][0])
                    }
                    else {
                        subcats[subcat_ids[i-1]][1].appendChild(subcats[subcat_ids[i]][0])
                    }
                }
            }
            var setting_def = control_panel.utility.get_setting_definition(key)
            var setting_func = function () {
                control_panel.values[key] = this.value
                control_panel.utility.trigger_callback(key)
            };
            var checkbox_func = function () {
                control_panel.values[key] = this.checked
                console.log("cb: "+this.checked);
                control_panel.utility.trigger_callback(key)
            };
            var func = setting_func;
            if (setting_def.type == "bool") {
                func = checkbox_func;
            }
            var setting_element = control_panel.dom_config.create_setting_element(key, setting_def.type, element, func)
            subcats[subcat_ids[keynum-1]][1].appendChild(setting_element)
        }
    }
    control_panel.settings_updater()
    control_panel.inits["settings"] = "done";
    all_inits = ["settings", "dom", "values"]
    if (all_inits.every(init => control_panel.inits[init] == "done")) {
        control_panel.initialized = true;
    }
}
control_panel.config_POST = false; //Set to true if we want to send the settings to the server
control_panel.settings_updated = function () {
    if (control_panel.config_POST) {
        var settings = control_panel.values
        var settings_str = JSON.stringify(settings)
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "/config_changed", true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.send(settings_str);
    }
}
control_panel.settings_updater = function () {
    //Prepare callbacks for all settings
    for (const key in control_panel.values) {
        if (Object.hasOwnProperty.call(control_panel.values, key)) {
            const element = control_panel.values[key];
            control_panel.utility.setup_callback(key, control_panel.settings_updated);
        }
    }
}
control_panel.utility.setup_group_callback = function(key, func) {
    var key_ = key;
    if (key_[0] != ".") {
        key_ = "." + key_;
    }
    for (const path in control_panel.values) {
        if (Object.hasOwnProperty.call(control_panel.values, path)) {
            if (path.startsWith(key_)) {
                control_panel.utility.setup_callback(path, func);
            }
        }
    }
}
control_panel.init_dom()
control_panel.setup_settings()
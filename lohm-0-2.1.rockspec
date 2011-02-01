package = "lohm"
version = "0-2.1"

source = {
   url = "https://github.com/slact/lua-ohm"
}

description = {
   summary = "Lua Object Hash Mapper",
   detailed = [[
     maps stuffs
   ]],
   homepage = "https://github.com/slact/lua-ohm",
   license = "MIT/X11"
}

dependencies = {
   "lua >= 5.1"
}

build = {
  type = "builtin",
  modules = {
    ["lohm"] = "lohm.lua",
    ["lohm.data"] = "lohm/data.lua",
    ["lohm.hash"] = "lohm/hash.lua",
    ["lohm.index"] = "lohm/index.lua",
    ["lohm.model"] = "lohm/model.lua",
    ["lohm.reference"] = "lohm/reference.lua",
    ["lohm.set"] = "lohm/set.lua",
  }
}
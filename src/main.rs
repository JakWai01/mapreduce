use std::env;
use std::fs;
use std::io::Write;

#[derive(Debug)]
struct KVStore<T, U> {
    key: T, value: U,
}

impl<T, U> KVStore<T, U> {
    fn new(key: T, value: U) -> Self {
        KVStore { key,
            value,
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();    
    
    let mut intermediate: Vec<KVStore<String, String>> = Vec::new();
    for file in &args[1..] {
        println!("{:?}", &file);
        
        let mut contents = fs::read_to_string(file).expect(format!("Could not read file: {}", stringify!(&file)).as_str());
        contents = sanitize_input(contents);
        
        let kva: Vec<KVStore<String, String>> = map(&file, &contents);
       
        for kv in kva {
            intermediate.push(kv)
        }
    }

    intermediate.sort_by_key(|i| i.key.clone());

    println!("{:?}", intermediate);

    let oname = "mr-out-0";
    let mut file = std::fs::OpenOptions::new().read(true).create(true).append(true).open(oname).unwrap();

    for i in 0..intermediate.len() {
        let mut j = i+1;
        while j < intermediate.len() && intermediate[j].key == intermediate[i].key {
            j = j+1;
        }

        let mut values: Vec<&String> = Vec::new();
        for k in i..j {
            values.push(&intermediate[k].value)
        }

        let output = reduce(&intermediate[i].key, values);

        if let Err(e) = write!(file, "{}", format!("{} {}\n", intermediate[i].key, output)) {
            eprintln!("Could not write to file: {}", e);   
        }
    }
}

fn map<'a>(_filename: &'a str, contents: &String) -> Vec<KVStore<String, String>> {   
    let words = contents.split(" ");
    
    let mut kva: Vec<KVStore<String, String>> = Vec::new();
    for word in words {
        kva.push(KVStore::new(String::from(word) , String::from("1")));
    }

    return kva
}

fn reduce(_key: &String, values: Vec<&String>) -> String {
    values.len().to_string()
}

fn sanitize_input(mut input: String) -> String {
    let len = input.trim_end_matches(&['\r', '\n'][..]).len();
    input.truncate(len);  
    input      
}

use std::env;
use std::fs;

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
    println!("Hello, world!");
    
    let args: Vec<String> = env::args().collect();    
    println!("{:?}", args);

    //
    // read each input file,
    // pass it to Map,
    // accumulate the intermediate Map output.
    //
    let mut intermediate: Vec<KVStore<&str, &str>> = Vec::new();
    for file in &args[1..] {
        println!("{:?}", &file);
        let contents = fs::read_to_string(file)
            .expect(format!("Could not read file: {}", stringify!(&file)).as_str());

        // println!("{}", &contents);

        let kva: Vec<KVStore<&str, &str>> = map(&file, &contents);

        for kv in kva {
            intermediate.push(kv)
        }
    }

    // 
    // a big difference from real MapReduce is that all the
    // intermediate data is in one place, intermediate[],
    // rather than being partinioned into NxM buckets.
    //

    //
    // call Reduce on each distinct key in intermediate[],
    // and print the result to mr-out-0.
    //
}

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
fn map<'a>(_filename: &'a str, contents: &'a str) -> Vec<KVStore<&'a str, &'a str>> {
    let words = contents.split(" ");
    
    let mut kva: Vec<KVStore<&str, &str>> = Vec::new();
    for word in words {
        kva.push(KVStore::new(word , "1"));
    }

    return kva
}

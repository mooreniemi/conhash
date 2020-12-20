use fake::{Dummy, Fake, Faker};
// using `faker` module with locales
use fake::faker::name::raw::*;
use fake::locales::*;
use rand;

#[derive(Debug, Dummy)]
pub struct OrderRecord {
    #[dummy(faker = "1000..2000")]
    order_id: usize,
    customer: String,
    paid: bool,
}

fn main() {
    for _ in 0..100 {
        let f: OrderRecord = Faker.fake();
        println!("{:?}", f);
    }
    let name_vec = fake::vec![String as Name(EN); 4, 3..5, 2];
    println!("random nested vec {:?}", name_vec)
}

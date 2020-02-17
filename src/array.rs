enum Arr {
    Int(Vec<i64>),
    Real(Vec<f64>),
    Str(Vec<String>),    
}

enum Scalar {
    Int(i64),
    Real(f64),
    Str(String),
}

enum Val {
    Arr,
    Scalar,
}

impl Arr {
    fn sum(&self) -> Option<Scalar> {
        match self {
            Arr::Int(ref v) =>  psum(v),
            Arr::Real(ref v) => psum(v),
        }
    }

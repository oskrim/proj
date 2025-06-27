def gf_add (a b : Nat) : Nat := a ^^^ b

theorem gf_add_example : gf_add 0x7 0xB = 12 := by
  rfl

def gf_mul_x (a : Nat) (k : Nat) (poly : Nat) : Nat :=
  let shifted := a <<< 1
  if shifted >= (1 <<< k) then
    shifted ^^^ poly
  else
    shifted

def gf_mul (a b : Nat) (k : Nat) (poly : Nat) : Nat :=
  let rec mul_aux (a b : Nat) (acc : Nat) (n : Nat) : Nat :=
    if n = 0 then acc
    else
      let acc' := if b &&& 1 = 1 then gf_add acc a else acc
      let a' := gf_mul_x a k poly
      let b' := b >>> 1
      mul_aux a' b' acc' (n - 1)
  mul_aux a b 0 k

def aes_poly : Nat := 0x11B
def aes_add (a b : Nat) : Nat := gf_add a b
def aes_mul (a b : Nat) : Nat := gf_mul a b 8 aes_poly

theorem gf_add_self_zero (a : Nat) : gf_add a a = 0 := by
  simp [gf_add, Nat.xor_self]

theorem gf_add_comm (a b : Nat) : gf_add a b = gf_add b a := by
  simp [gf_add, Nat.xor_comm]

theorem gf_add_assoc (a b c : Nat) : gf_add (gf_add a b) c = gf_add a (gf_add b c) := by
  simp [gf_add, Nat.xor_assoc]

theorem gf_add_zero (a : Nat) : gf_add a 0 = a := by
  simp [gf_add]

theorem gf_add_inverse (a : Nat) : gf_add a a = 0 := by
  simp [gf_add, Nat.xor_self]

theorem gf_mul_zero (a : Nat) (k : Nat) (poly : Nat) : gf_mul a 0 k poly = 0 := by sorry

theorem gf_mul_one (a : Nat) (k : Nat) (poly : Nat) (h : a < 2^k) : gf_mul a 1 k poly = a := by sorry

theorem gf_mul_comm (a b : Nat) (k : Nat) (poly : Nat) : gf_mul a b k poly = gf_mul b a k poly := by sorry

theorem gf_mul_assoc (a b c : Nat) (k : Nat) (poly : Nat) :
  gf_mul (gf_mul a b k poly) c k poly = gf_mul a (gf_mul b c k poly) k poly := by sorry

theorem gf_mul_distributive (a b c : Nat) (k : Nat) (poly : Nat) :
  gf_mul a (gf_add b c) k poly = gf_add (gf_mul a b k poly) (gf_mul a c k poly) := by sorry

theorem gf_mul_bounded (a b : Nat) (k : Nat) (poly : Nat) (ha : a < 2^k) (hb : b < 2^k) :
  gf_mul a b k poly < 2^k := by sorry

theorem gf_add_bounded (a b : Nat) (k : Nat) (ha : a < 2^k) (hb : b < 2^k) :
  gf_add a b < 2^k := by sorry

theorem gf_inverse_exists (a : Nat) (k : Nat) (poly : Nat) (ha : 0 < a) (ha_bound : a < 2^k) :
  ∃ b : Nat, b < 2^k ∧ gf_mul a b k poly = 1 := by sorry

theorem gf_inverse_unique (a b c : Nat) (k : Nat) (poly : Nat)
  (hb : b < 2^k) (hc : c < 2^k) (hb_inv : gf_mul a b k poly = 1) (hc_inv : gf_mul a c k poly = 1) :
  b = c := by sorry

theorem aes_mul_x_squared (a : Nat) (h : a < 256) :
  aes_mul (aes_mul a 2) 2 = aes_mul a 4 := by sorry

theorem aes_mul_by_3 (a : Nat) :
  aes_mul a 3 = gf_add (aes_mul a 2) a := by sorry

-- theorem aes_fermat (a : Nat) (ha : 0 < a) (ha_bound : a < 256) :
--   let a254 := Nat.iterate (fun x => aes_mul x a) 254 1
--   aes_mul a a254 = 1 := by sorry

-- theorem aes_sbox_inverse_property (a : Nat) (ha : 0 < a) (ha_bound : a < 256) :
--   let inv := Nat.iterate (fun x => aes_mul x a) 254 1
--   aes_mul a inv = 1 := by sorry

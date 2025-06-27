-- 1.1
def mod5_mult (a b : Nat) : Nat := (a * b) % 5

def S4 : List Nat := [1, 2, 3, 4]

def inverse_val (n : Nat) : Nat :=
  match n with
  | 1 => 1 | 2 => 3 | 3 => 2 | 4 => 4
  | _ => 1

theorem inverse_val_in_S4 (n : Nat) (h : n ∈ S4) : inverse_val n ∈ S4 := by
  simp [S4] at h; rcases h with (rfl | rfl | rfl | rfl) <;> simp [inverse_val, S4]

theorem all_elements_have_inverses (a : Nat) (x : a ∈ S4) : mod5_mult a (inverse_val a) = 1 := by
  simp [S4] at x; rcases x with (rfl | rfl | rfl | rfl) <;> decide

theorem closure (a b : Nat) (ha : a ∈ S4) (hb : b ∈ S4) : mod5_mult a b ∈ S4 := by
  simp [S4] at ha hb; rcases ha with (rfl | rfl | rfl| rfl) <;> rcases hb with (rfl | rfl | rfl | rfl) <;> simp [mod5_mult] <;> decide

theorem left_identity (a : Nat) (ha : a ∈ S4) : mod5_mult 1 a = a := by
  simp [mod5_mult, S4] at ha ⊢
  omega

theorem right_identity (a : Nat) (ha : a ∈ S4) : mod5_mult a 1 = a := by
  simp [mod5_mult, S4] at ha ⊢
  omega

theorem associativity (a b c : Nat) (ha : a ∈ S4) (hb : b ∈ S4) (hc : c ∈ S4) :
    mod5_mult (mod5_mult a b) c = mod5_mult a (mod5_mult b c) := by
  simp [S4] at ha hb hc
  rcases ha with (rfl | rfl | rfl | rfl) <;>
  rcases hb with (rfl | rfl | rfl | rfl) <;>
  rcases hc with (rfl | rfl | rfl | rfl) <;>
  simp [mod5_mult]

-- 1.2
def mod4_mult (a b : Nat) : Nat := (a * b) % 4

def S3 : List Nat := [1, 2, 3]

theorem two_has_no_inverse_omega : ¬∃ b ∈ S3, mod4_mult 2 b = 1 := by
  intro ⟨b, hb, h⟩
  simp [S3] at hb
  simp [mod4_mult] at h
  omega
